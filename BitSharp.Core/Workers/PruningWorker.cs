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
using System.Threading;

namespace BitSharp.Core.Workers
{
    internal class PruningWorker : Worker
    {
        private readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly ICoreDaemon coreDaemon;
        private readonly IStorageManager storageManager;
        private readonly ChainStateWorker chainStateWorker;

        private readonly WorkerPool gatherWorkers;
        private readonly WorkerPool pruneBlockWorkers;

        private readonly AverageMeasure txCountMeasure = new AverageMeasure();
        private readonly AverageMeasure txRateMeasure = new AverageMeasure();
        private readonly DurationMeasure totalDurationMeasure = new DurationMeasure();
        private readonly DurationMeasure gatherIndexDurationMeasure = new DurationMeasure();
        private readonly DurationMeasure pruneIndexDurationMeasure = new DurationMeasure();
        private readonly DurationMeasure pruneBlocksDurationMeasure = new DurationMeasure();
        private readonly DurationMeasure flushDurationMeasure = new DurationMeasure();
        private readonly DurationMeasure commitDurationMeasure = new DurationMeasure();

        //TODO
        private ChainBuilder prunedChain;

        public PruningWorker(WorkerConfig workerConfig, ICoreDaemon coreDaemon, IStorageManager storageManager, ChainStateWorker chainStateWorker)
            : base("PruningWorker", workerConfig.initialNotify, workerConfig.minIdleTime, workerConfig.maxIdleTime)
        {
            this.coreDaemon = coreDaemon;
            this.storageManager = storageManager;
            this.chainStateWorker = chainStateWorker;

            this.prunedChain = new ChainBuilder();
            this.Mode = PruningMode.None;

            var gatherThreadCount = Environment.ProcessorCount * 2;
            this.gatherWorkers = new WorkerPool("PruningWorker.GatherWorkers", gatherThreadCount);

            // initialize a pool of pruning workers
            //TODO
            var txesPruneThreadCount = storageManager.GetType().Name.Contains("Lmdb") ? 1 : Environment.ProcessorCount * 2;
            this.pruneBlockWorkers = new WorkerPool("PruningWorker.PruneBlockWorkers", txesPruneThreadCount);
        }

        protected override void SubDispose()
        {
            this.gatherWorkers.Dispose();
            this.pruneBlockWorkers.Dispose();
            this.txCountMeasure.Dispose();
            this.txRateMeasure.Dispose();
            this.totalDurationMeasure.Dispose();
            this.gatherIndexDurationMeasure.Dispose();
            this.pruneIndexDurationMeasure.Dispose();
            this.pruneBlocksDurationMeasure.Dispose();
            this.flushDurationMeasure.Dispose();
            this.commitDurationMeasure.Dispose();
        }

        public PruningMode Mode { get; set; }

        public int PrunableHeight { get; set; }

        private DateTime lastLogTime = DateTime.Now;
        protected override void WorkAction()
        {
            // check if pruning is turned off
            if (this.Mode == PruningMode.None)
                return;

            var totalStopwatch = Stopwatch.StartNew();
            var gatherIndexStopwatch = new Stopwatch();
            var pruneIndexStopwatch = new Stopwatch();
            var pruneBlocksStopwatch = new Stopwatch();
            var flushStopwatch = new Stopwatch();
            var commitStopwatch = new Stopwatch();

            var startHeight = this.prunedChain.Height;
            var stopHeight = this.prunedChain.Height;
            var txCount = 0;

            // get the current processed chain
            var processedChain = this.coreDaemon.CurrentChain;

            // ensure chain state is flushed before pruning
            //TODO: needed if the chain state hasn't been flushed to the point where what has
            //      been flushed is behind the safe pruning buffer, on crash chain state would
            //      then need block txes data that has been removed
            //flushStopwatch.Time(() =>
            //{
            //    chainStateCursor.Flush();
            //});

            var prunedBlockIndices = new ConcurrentSet<int>();
            var spentTxHashes = new ConcurrentSet<UInt256>();

            // navigate from the current pruned chain towards the current processed chain
            var isLagging = false;
            foreach (var pathElement in this.prunedChain.ToImmutable().NavigateTowards(processedChain))
            {
                // cooperative loop
                if (!this.IsStarted)
                    break;

                // check if pruning is turned off
                var mode = this.Mode;
                if (mode == PruningMode.None)
                    break;

                // get candidate block to be pruned
                var direction = pathElement.Item1;
                var chainedHeader = pathElement.Item2;

                // determine maximum safe pruning height, based on a buffer distance from the processed chain height
                var blocksPerDay = 144;
                var pruneBuffer = blocksPerDay * 7;
                var maxHeight = processedChain.Height - pruneBuffer;
                //TODO
                maxHeight = Math.Min(maxHeight, this.PrunableHeight);

                // check if this block is safe to prune
                if (chainedHeader.Height > maxHeight)
                {
                    //TODO better way to block chain state worker when pruning is behind
                    if (this.chainStateWorker != null)
                    {
                        this.chainStateWorker.Start();
                        this.chainStateWorker.NotifyWork();
                    }
                    break;
                }

                if (direction > 0)
                {
                    // prune the block
                    int pruneBlockTxCount;
                    this.PruneBlock(mode, processedChain, chainedHeader, prunedBlockIndices, spentTxHashes, out pruneBlockTxCount, gatherIndexStopwatch, pruneIndexStopwatch, pruneBlocksStopwatch);
                    txCount += pruneBlockTxCount;
                    txCountMeasure.Tick(pruneBlockTxCount);

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
                    Debugger.Break();
                    throw new InvalidOperationException();
                }

                stopHeight = this.prunedChain.Height;

                // limit how long pruning transaction is kept open
                if (totalStopwatch.Elapsed > TimeSpan.FromSeconds(15))
                {
                    //TODO better way to block chain state worker when pruning is behind
                    this.logger.Info("Pausing chain state processing, pruning is lagging.");
                    if (this.chainStateWorker != null)
                        this.chainStateWorker.Stop();

                    this.ForceWork();
                    isLagging = true;
                    break;
                }
            }

            // blocks must be flushed as the pruning information has been removed from the chain state
            // if the system crashed and the pruned chain state was persisted while the pruned blocks were not,
            // the information to prune them again would be lost
            flushStopwatch.Time(() =>
            {
                //this.storageManager.BlockTxesStorage.Flush();
                flushStopwatch.Stop();
            });

            using (var handle = this.storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                chainStateCursor.BeginTransaction();
                var wasCommitted = false;
                try
                {
                    pruneIndexStopwatch.Time(() =>
                    {
                        foreach (var height in prunedBlockIndices)
                            chainStateCursor.TryRemoveBlockSpentTxes(height);
                        foreach (var txHash in spentTxHashes)
                            chainStateCursor.TryRemoveUnspentTx(txHash);
                    });

                    // commit pruned chain state
                    // flush is not needed here, at worst pruning will be performed again against already pruned transactions
                    commitStopwatch.Time(() =>
                    {
                        chainStateCursor.CommitTransaction();
                        wasCommitted = true;
                    });
                }
                finally
                {
                    if (!wasCommitted)
                        chainStateCursor.RollbackTransaction();
                }
            }

            // log if pruning was done
            if (stopHeight > startHeight)
            {
                var txRate = txCount / totalStopwatch.Elapsed.TotalSeconds;
                txRateMeasure.Tick((float)txRate);
                gatherIndexDurationMeasure.Tick(gatherIndexStopwatch.Elapsed);
                pruneIndexDurationMeasure.Tick(pruneIndexStopwatch.Elapsed);
                pruneBlocksDurationMeasure.Tick(pruneBlocksStopwatch.Elapsed);
                flushDurationMeasure.Tick(flushStopwatch.Elapsed);
                commitDurationMeasure.Tick(commitStopwatch.Elapsed);
                totalDurationMeasure.Tick(totalStopwatch.Elapsed);

                if (isLagging || DateTime.Now - lastLogTime > TimeSpan.FromSeconds(15))
                {
                    this.logger.Debug(measure.GetAverage().TotalMilliseconds);

                    lastLogTime = DateTime.Now;
                    this.logger.Info(
@"Pruned from block {0:#,##0} to {1:#,##0}:
- tx count:       {2,8:#,##0}
- tx count/block: {3,8:#,##0}
- avg tx rate:    {4,8:#,##0}/s
- gather index:   {5,12:#,##0.000}s
- prune blocks:   {6,12:#,##0.000}s
- prune index:    {7,12:#,##0.000}s
- flush:          {8,12:#,##0.000}s
- commit:         {9,12:#,##0.000}s
- TOTAL:          {10,12:#,##0.000}s"
                        .Format2(startHeight, stopHeight, txCount, txCountMeasure.GetAverage(), txRateMeasure.GetAverage(), gatherIndexDurationMeasure.GetAverage().TotalSeconds, pruneBlocksDurationMeasure.GetAverage().TotalSeconds, pruneIndexDurationMeasure.GetAverage().TotalSeconds, flushDurationMeasure.GetAverage().TotalSeconds, commitDurationMeasure.GetAverage().TotalSeconds, totalDurationMeasure.GetAverage().TotalSeconds));
                }
            }

            //this.logger.Info("Finished round of pruning.");

            //TODO add periodic stats logging like ChainStateBuilder has
        }

        private DurationMeasure measure = new DurationMeasure();
        private void PruneBlock(PruningMode mode, Chain chain, ChainedHeader pruneBlock, ConcurrentSet<int> prunedBlockIndices, ConcurrentSet<UInt256> spentTxHashes, out int txCount, Stopwatch gatherIndexStopwatch, Stopwatch pruneIndexStopwatch, Stopwatch pruneBlocksStopwatch)
        {
            //TODO the replay information about blocks that have been rolled back also needs to be pruned (UnmintedTx)

            var txCountLocal = 0;

            // retrieve the spent txes for this block
            IImmutableList<UInt256> spentTxes;
            using (var handle = this.storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                chainStateCursor.BeginTransaction(readOnly: true);
                try
                {
                    chainStateCursor.TryGetBlockSpentTxes(pruneBlock.Height, out spentTxes);
                }
                finally
                {
                    chainStateCursor.RollbackTransaction();
                }
            }

            if (spentTxes != null)
            {
                // dictionary to keep track of spent transactions against their block
                var pruneData = new ConcurrentDictionary<int, ConcurrentBag<int>>();

                gatherIndexStopwatch.Time(() =>
                {
                    using (var spentTxesQueue = new ConcurrentBlockingQueue<UInt256>())
                    {
                        spentTxesQueue.AddRange(spentTxes);
                        spentTxesQueue.CompleteAdding();

                        this.gatherWorkers.Do(() =>
                        {
                            var emptyBag = new ConcurrentBag<int>();
                            using (var handle = this.storageManager.OpenChainStateCursor())
                            {
                                var chainStateCursor = handle.Item;

                                chainStateCursor.BeginTransaction(readOnly: true);
                                try
                                {
                                    foreach (var spentTxHash in spentTxesQueue.GetConsumingEnumerable())
                                    {
                                        Interlocked.Increment(ref txCountLocal);

                                        UnspentTx spentTx;
                                        if (chainStateCursor.TryGetUnspentTx(spentTxHash, out spentTx))
                                        {
                                            // store spent tx to be removed from chain state
                                            spentTxHashes.Add(spentTxHash);

                                            // queue up spent tx to be pruned from block txes
                                            if (mode == PruningMode.ReplayAndRollbackAndTxes)
                                            {
                                                ConcurrentBag<int> blockTxIndices;
                                                if (pruneData.TryAdd(spentTx.BlockIndex, emptyBag))
                                                {
                                                    blockTxIndices = emptyBag;
                                                    emptyBag = new ConcurrentBag<int>();
                                                }
                                                else
                                                    blockTxIndices = pruneData[spentTx.BlockIndex];

                                                blockTxIndices.Add(spentTx.TxIndex);
                                            }
                                        }
                                    }
                                }
                                finally
                                {
                                    chainStateCursor.RollbackTransaction();
                                }
                            }
                        });
                    }
                });

                // store block's spent txes to be removed from chain state
                if (mode == PruningMode.ReplayAndRollback || mode == PruningMode.ReplayAndRollbackAndTxes)
                {
                    prunedBlockIndices.Add(pruneBlock.Height);
                }

                // remove spent transactions from block storage
                if (mode == PruningMode.ReplayAndRollbackAndTxes && pruneData.Count > 0)
                {
                    pruneBlocksStopwatch.Time(() =>
                    {
                        using (var blockTxesPruneQueue = new ConcurrentBlockingQueue<KeyValuePair<UInt256, IEnumerable<int>>>())
                        using (this.pruneBlockWorkers.Start(() =>
                        {
                            this.storageManager.BlockTxesStorage.PruneElements(blockTxesPruneQueue.GetConsumingEnumerable());
                            //this.storageManager.BlockTxesStorage.DeleteElements(blockTxesPruneQueue.GetConsumingEnumerable());
                        }))
                        {
                            foreach (var keyPair in pruneData)
                            {
                                var confirmedBlockIndex = keyPair.Key;
                                var confirmedBlockHash = chain.Blocks[confirmedBlockIndex].Hash;
                                var spentTxIndices = keyPair.Value;

                                blockTxesPruneQueue.Add(new KeyValuePair<UInt256, IEnumerable<int>>(confirmedBlockHash, spentTxIndices));
                            }

                            blockTxesPruneQueue.CompleteAdding();
                        }
                    });
                }
            }
            else if (pruneBlock.Height > 0)
            {
                //TODO can't throw an exception unless the pruned chain is persisted
                //this.logger.Info("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX: {0:#,##0}".Format2(pruneBlock.Height));
                //throw new InvalidOperationException();
            }

            txCount = txCountLocal;
        }
    }
}
