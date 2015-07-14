using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BitSharp.Core.Workers
{
    internal class PruningWorker : Worker
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly ICoreDaemon coreDaemon;
        private readonly IStorageManager storageManager;
        private readonly ChainStateWorker chainStateWorker;

        private readonly AverageMeasure txCountMeasure = new AverageMeasure();
        private readonly AverageMeasure txRateMeasure = new AverageMeasure();
        private readonly DurationMeasure totalDurationMeasure = new DurationMeasure();
        private readonly DurationMeasure pruneIndexDurationMeasure = new DurationMeasure();
        private readonly DurationMeasure pruneBlocksDurationMeasure = new DurationMeasure();

        //TODO
        private ChainBuilder prunedChain;

        private int lastLogHeight = -1;
        private bool lagLogged;

        public PruningWorker(WorkerConfig workerConfig, ICoreDaemon coreDaemon, IStorageManager storageManager, ChainStateWorker chainStateWorker)
            : base("PruningWorker", workerConfig.initialNotify, workerConfig.minIdleTime, workerConfig.maxIdleTime)
        {
            this.coreDaemon = coreDaemon;
            this.storageManager = storageManager;
            this.chainStateWorker = chainStateWorker;

            this.prunedChain = new ChainBuilder();
            this.Mode = PruningMode.None;

            // initialize a pool of pruning workers
            var txesPruneThreadCount = Environment.ProcessorCount * 2;
        }

        protected override void SubDispose()
        {
            this.txCountMeasure.Dispose();
            this.txRateMeasure.Dispose();
            this.totalDurationMeasure.Dispose();
            this.pruneIndexDurationMeasure.Dispose();
            this.pruneBlocksDurationMeasure.Dispose();
        }

        public PruningMode Mode { get; set; }

        public int PrunableHeight { get; set; }

        protected override void WorkAction()
        {
            // check if pruning is turned off
            var mode = this.Mode;
            if (mode == PruningMode.None)
                return;

            // navigate from the current pruned chain towards the current processed chain
            foreach (var pathElement in this.prunedChain.ToImmutable().NavigateTowards(() => this.coreDaemon.CurrentChain))
            {
                // cooperative loop
                if (!this.IsStarted)
                    break;

                // get candidate block to be pruned
                var direction = pathElement.Item1;
                var chainedHeader = pathElement.Item2;

                // get the current processed chain
                var processedChain = this.coreDaemon.CurrentChain;

                // determine maximum safe pruning height, based on a buffer distance from the processed chain height
                var blocksPerDay = 144;
                //TODO there should also be a buffer between when blocks are pruned, and when the pruning information
                //TODO to perform that operation is removed (tx index, spent txes)
                var pruneBuffer = blocksPerDay * 7;
                var maxHeight = processedChain.Height - pruneBuffer;
                //TODO
                maxHeight = Math.Min(maxHeight, this.PrunableHeight);

                // check if this block is safe to prune
                if (chainedHeader.Height > maxHeight)
                    break;

                if (direction > 0)
                {
                    // prune the block
                    this.PruneBlock(mode, processedChain, chainedHeader);

                    // track pruned block
                    this.prunedChain.AddBlock(chainedHeader);
                }
                else if (direction < 0)
                {
                    // pruning should not roll back, the buffer is in place to prevent this on orphans
                    throw new InvalidOperationException();
                }
                else
                {
                    throw new InvalidOperationException();
                }

                // pause chain state processing when pruning lags too far behind
                var isLagging = maxHeight - chainedHeader.Height > 100;
                if (isLagging)
                {
                    Throttler.IfElapsed(TimeSpan.FromSeconds(5), () =>
                    {
                        logger.Info("Pruning is lagging.");
                        lagLogged = true;
                    });

                    //TODO better way to block chain state worker when pruning is behind
                    if (this.chainStateWorker != null && this.chainStateWorker.IsStarted)
                        this.chainStateWorker.Stop(TimeSpan.Zero);
                }
                else
                {
                    //TODO better way to block chain state worker when pruning is behind
                    if (this.chainStateWorker != null && !this.chainStateWorker.IsStarted)
                    {
                        this.chainStateWorker.NotifyAndStart();
                        if (lagLogged)
                        {
                            logger.Info("Pruning is caught up.");
                            lagLogged = false;
                        }
                    }
                }

                // log pruning stats periodically
                Throttler.IfElapsed(TimeSpan.FromSeconds(15), () =>
                {
                    logger.Info(
@"Pruned from block {0:N0} to {1:N0}:
- avg tx rate:    {2,8:N0}/s
- per block stats:
- tx count:       {3,8:N0}
- prune blocks:   {4,12:N3}ms
- prune index:    {5,12:N3}ms
- TOTAL:          {6,12:N3}ms"
                        .Format2(lastLogHeight, chainedHeader.Height, txRateMeasure.GetAverage(), txCountMeasure.GetAverage(), pruneBlocksDurationMeasure.GetAverage().TotalMilliseconds, pruneIndexDurationMeasure.GetAverage().TotalMilliseconds, totalDurationMeasure.GetAverage().TotalMilliseconds));

                    lastLogHeight = chainedHeader.Height + 1;
                });
            }

            // ensure chain state processing is resumed
            if (this.chainStateWorker != null && !this.chainStateWorker.IsStarted)
                this.chainStateWorker.NotifyAndStart();
        }

        private void PruneBlock(PruningMode mode, Chain chain, ChainedHeader pruneBlock)
        {
            //TODO the replay information about blocks that have been rolled back also needs to be pruned (UnmintedTx)

            var txCount = 0;
            var totalStopwatch = Stopwatch.StartNew();
            var pruneIndexStopwatch = new Stopwatch();
            var pruneBlocksStopwatch = new Stopwatch();

            // retrieve the spent txes for this block
            BlockSpentTxes spentTxes;
            using (var handle = this.storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                chainStateCursor.BeginTransaction(readOnly: true);
                chainStateCursor.TryGetBlockSpentTxes(pruneBlock.Height, out spentTxes);
            }

            if (spentTxes != null)
            {
                txCount = spentTxes.Count;

                // begin prune block txes (either merkle prune or delete)
                pruneBlocksStopwatch.Start();
                var pruneBlockTxesTask = PruneBlockTxes(mode, chain, pruneBlock, spentTxes);
                var timerTask = pruneBlockTxesTask.ContinueWith(_ => pruneBlocksStopwatch.Stop());

                // prune tx index
                pruneIndexStopwatch.Time(() =>
                    PruneTxIndex(mode, chain, pruneBlock, spentTxes));

                // wait for block txes prune to finish
                pruneBlockTxesTask.Wait();
                timerTask.Wait();

                // remove block spent txes information
                //TODO should have a buffer on removing this, block txes pruning may need it again if flush doesn't happen
                PruneBlockSpentTxes(mode, chain, pruneBlock);
            }
            else //if (pruneBlock.Height > 0)
            {
                //TODO can't throw an exception unless the pruned chain is persisted
                //logger.Info("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX: {0:#,##0}".Format2(pruneBlock.Height));
                //throw new InvalidOperationException();
                txCount = 0;
            }

            // track stats
            txCountMeasure.Tick(txCount);
            txRateMeasure.Tick((float)(txCount / totalStopwatch.Elapsed.TotalSeconds));
            pruneIndexDurationMeasure.Tick(pruneIndexStopwatch.Elapsed);
            pruneBlocksDurationMeasure.Tick(pruneBlocksStopwatch.Elapsed);
            totalDurationMeasure.Tick(totalStopwatch.Elapsed);
        }

        private Task PruneBlockTxes(PruningMode mode, Chain chain, ChainedHeader pruneBlock, BlockSpentTxes spentTxes)
        {
            if (!mode.HasFlag(PruningMode.BlockTxesPreserveMerkle) && !mode.HasFlag(PruningMode.BlockTxesDestroyMerkle))
                return Task.FromResult(false); //TODO Task.CompletedTask

            // create a source of txes to prune sources, for each block
            var pruningQueue = new BufferBlock<Tuple<int, List<int>>>();

            // prepare tx pruner, to prune a txes source for a given block
            var txPruner = new ActionBlock<Tuple<int, List<int>>>(
                blockWorkItem =>
                {
                    var blockIndex = blockWorkItem.Item1;
                    var blockHash = chain.Blocks[blockIndex].Hash;
                    var spentTxIndices = blockWorkItem.Item2;
                    var pruneWorkItem = new KeyValuePair<UInt256, IEnumerable<int>>(blockHash, spentTxIndices);

                    if (mode.HasFlag(PruningMode.BlockTxesPreserveMerkle))
                        this.storageManager.BlockTxesStorage.PruneElements(new[] { pruneWorkItem });
                    else
                        this.storageManager.BlockTxesStorage.DeleteElements(new[] { pruneWorkItem });
                },
                new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 64 });

            pruningQueue.LinkTo(txPruner, new DataflowLinkOptions { PropagateCompletion = true });

            // queue spent txes, grouped by block
            var queueSpentTxes = Task.Run(() =>
            {
                try
                {
                    foreach (var spentTxesByBlock in spentTxes.ReadByBlock())
                    {
                        var blockIndex = spentTxesByBlock.Item1;
                        var txIndices = spentTxesByBlock.Item2.Select(x => x.TxIndex).ToList();

                        pruningQueue.Post(Tuple.Create(blockIndex, txIndices));
                    }
                }
                finally
                {
                    // complete overall pruning queue
                    pruningQueue.Complete();
                }
            });

            return txPruner.Completion;
        }

        private void PruneTxIndex(PruningMode mode, Chain chain, ChainedHeader pruneBlock, BlockSpentTxes spentTxes)
        {
            if (!mode.HasFlag(PruningMode.TxIndex))
                return;

            var maxParallelism = 64;

            // prepare a cache of cursors to be used by the pruning action block, allowing a pool of transactions
            var openedCursors = new ConcurrentBag<IChainStateCursor>();
            using (var cursorHandles = new DisposableCache<DisposeHandle<IChainStateCursor>>(maxParallelism,
                () =>
                {
                    // retrieve a new cursor and start its transaction, keeping track of any cursors opened
                    var cursorHandle = this.storageManager.OpenChainStateCursor();
                    cursorHandle.Item.BeginTransaction();
                    openedCursors.Add(cursorHandle.Item);

                    return cursorHandle;
                }))
            {
                var pruneTxIndex = new ActionBlock<SpentTx>(
                    spentTx =>
                    {
                        using (var handle = cursorHandles.TakeItem())
                        {
                            var chainStateCursor = handle.Item.Item;

                            if (!chainStateCursor.InTransaction)
                            {
                                chainStateCursor.BeginTransaction();
                                openedCursors.Add(chainStateCursor);
                            }

                            chainStateCursor.TryRemoveUnspentTx(spentTx.TxHash);
                        }
                    },
                    new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = maxParallelism });

                var spentTxesQueue = new BufferBlock<SpentTx>();
                spentTxesQueue.LinkTo(pruneTxIndex, new DataflowLinkOptions { PropagateCompletion = true });
                spentTxesQueue.PostAndComplete(spentTxes);

                pruneTxIndex.Completion.Wait();

                // commit all opened cursors on success
                foreach (var cursor in openedCursors)
                    cursor.CommitTransaction();
            }
        }

        private void PruneBlockSpentTxes(PruningMode mode, Chain chain, ChainedHeader pruneBlock)
        {
            if (!mode.HasFlag(PruningMode.BlockSpentIndex))
                return;

            using (var handle = this.storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                chainStateCursor.BeginTransaction();
                
                // TODO don't immediately remove list of spent txes per block from chain state,
                //      use an additional safety buffer in case there was an issue pruning block
                //      txes (e.g. didn't flush and crashed), keeping the information  will allow
                //      the block txes pruning to be performed again
                chainStateCursor.TryRemoveBlockSpentTxes(pruneBlock.Height);

                chainStateCursor.CommitTransaction();
            }
        }
    }
}
