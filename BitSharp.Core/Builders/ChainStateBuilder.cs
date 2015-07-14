using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using NLog;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BitSharp.Core.Builders
{
    internal class ChainStateBuilder : IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly IBlockchainRules rules;
        private readonly ICoreStorage coreStorage;
        private readonly IStorageManager storageManager;

        private readonly ReaderWriterLockSlim commitLock = new ReaderWriterLockSlim();
        private Chain chain;
        private readonly UtxoBuilder utxoBuilder;

        private readonly BuilderStats stats;

        public ChainStateBuilder(IBlockchainRules rules, ICoreStorage coreStorage, IStorageManager storageManager)
        {
            this.rules = rules;
            this.coreStorage = coreStorage;
            this.storageManager = storageManager;

            this.stats = new BuilderStats();

            using (var handle = this.storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                chainStateCursor.BeginTransaction(readOnly: true);
                var chainTip = chainStateCursor.ChainTip;

                Chain chainTipChain;
                if (!TryReadChain(chainStateCursor, chainTip != null ? chainTip.Hash : null, out chainTipChain))
                    throw new InvalidOperationException();

                this.chain = chainTipChain;
            }
            this.utxoBuilder = new UtxoBuilder();

            // thread count for i/o task (TxLoader)
            var ioThreadCount = Environment.ProcessorCount * 2;
        }

        public void Dispose()
        {
            this.stats.Dispose();
            this.commitLock.Dispose();
        }

        public Chain Chain
        {
            get
            {
                return this.chain;
            }
        }

        public BuilderStats Stats { get { return this.stats; } }

        public void AddBlock(ChainedHeader chainedHeader, IEnumerable<Transaction> transactions)
        {
            AddBlock(chainedHeader, transactions.Select((tx, txIndex) => new BlockTx(txIndex, depth: 0, hash: tx.Hash, pruned: false, transaction: tx)));
        }

        public void AddBlock(ChainedHeader chainedHeader, IEnumerable<BlockTx> blockTxes)
        {
            var stopwatch = Stopwatch.StartNew();

            using (var handle = this.storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                chainStateCursor.BeginTransaction();

                // verify storage chain tip matches this chain state builder's chain tip
                if (!(chainStateCursor.ChainTip == null && chain.Height == -1)
                    && (chainStateCursor.ChainTip.Hash != chain.LastBlock.Hash))
                {
                    throw new InvalidOperationException("ChainStateBuilder is out of sync with underlying storage.");
                }

                var newChain = this.chain.ToBuilder().AddBlock(chainedHeader).ToImmutable();

                using (var chainState = new ChainState(this.chain, this.storageManager))
                using (var deferredChainStateCursor = new DeferredChainStateCursor(chainState))
                {
                    // begin reading block txes into the buffer
                    var blockTxesBuffer = new BufferBlock<BlockTx>();
                    var sendBlockTxes = blockTxesBuffer.SendAndCompleteAsync(blockTxes);
                    sendBlockTxes.ContinueWith(_ => this.stats.txesReadDurationMeasure.Tick(stopwatch.Elapsed));

                    // warm-up utxo entries for block txes
                    var warmedBlockTxes = UtxoLookAhead.LookAhead(blockTxesBuffer, deferredChainStateCursor);

                    // track when the overall look ahead completes
                    warmedBlockTxes.Completion.ContinueWith(_ => this.stats.lookAheadDurationMeasure.Tick(stopwatch.Elapsed));

                    // begin calculating the utxo updates
                    var loadingTxes = CalculateUtxo(newChain, warmedBlockTxes, deferredChainStateCursor);
                    loadingTxes.Completion.ContinueWith(_ =>
                        deferredChainStateCursor.CompleteWorkQueue());

                    // keep count of total input previous transactions that need to be loaded
                    var totalTxInputCount = 0;
                    var totalTxInputCounter = new TransformBlock<LoadingTx, LoadingTx>(loadingTx =>
                    {
                        if (!loadingTx.IsCoinbase)
                            totalTxInputCount += loadingTx.Transaction.Inputs.Length;

                        return loadingTx;
                    });
                    loadingTxes.LinkTo(totalTxInputCounter, new DataflowLinkOptions { PropagateCompletion = true });

                    // begin loading txes
                    var loadedTxes = TxLoader.LoadTxes(coreStorage, totalTxInputCounter);

                    // keep count of input previous transactions that have been loaded
                    var loadedTxInputCount = 0;
                    var loadedTxInputsCounter = new TransformBlock<LoadedTx, LoadedTx>(loadedTx =>
                    {
                        if (!loadedTx.IsCoinbase)
                            loadedTxInputCount += loadedTx.InputTxes.Length;

                        return loadedTx;
                    });
                    loadedTxes.LinkTo(loadedTxInputsCounter, new DataflowLinkOptions { PropagateCompletion = true });

                    // track how many transactions are waiting to be loaded when utxo calculation completes
                    totalTxInputCounter.Completion.ContinueWith(_ =>
                        this.stats.pendingTxesAtCompleteAverageMeasure.Tick(totalTxInputCount - loadedTxInputCount));

                    // begin validating the block
                    var blockValidator = BlockValidator.ValidateBlock(coreStorage, rules, chainedHeader, loadedTxInputsCounter);

                    //TODO note - the utxo updates could be applied either while validation is ongoing, or after it is complete
                    this.stats.applyUtxoDurationMeasure.MeasureIf(chainedHeader.Height > 0, () =>
                    {
                        // apply the changes, do not yet commit
                        deferredChainStateCursor.ApplyChangesToParent(chainStateCursor);
                        chainStateCursor.ChainTip = chainedHeader;
                        chainStateCursor.TryAddHeader(chainedHeader);
                    });

                    // wait for block validation to complete, any exceptions that ocurred will be thrown
                    this.stats.waitToCompleteDurationMeasure.MeasureIf(chainedHeader.Height > 0, () =>
                    {
                        sendBlockTxes.Wait();
                        blockTxesBuffer.Completion.Wait();
                        warmedBlockTxes.Completion.Wait();
                        loadingTxes.Completion.Wait();
                        loadedTxes.Completion.Wait();
                        blockValidator.Wait();
                    });
                }

                this.stats.commitUtxoDurationMeasure.MeasureIf(chainedHeader.Height > 0, () =>
                {
                    // only commit the utxo changes once block validation has completed
                    this.commitLock.DoWrite(() =>
                    {
                        chainStateCursor.CommitTransaction();
                        this.chain = newChain;
                    });
                });

                // MEASURE: Block Rate
                if (chainedHeader.Height > 0)
                {
                    this.stats.blockRateMeasure.Tick();
                    stats.addBlockDurationMeasure.Tick(stopwatch.Elapsed);
                }
            }

            this.LogBlockchainProgress();
        }

        private ISourceBlock<LoadingTx> CalculateUtxo(Chain newChain, ISourceBlock<BlockTx> blockTxes, IChainStateCursor chainStateCursor)
        {
            var calcUtxoStopwatch = Stopwatch.StartNew();
            var pendingTxCount = 0;

            // calculate the new block utxo, only output availability is checked and updated
            var loadingTxes = this.utxoBuilder.CalculateUtxo(chainStateCursor, newChain, blockTxes);

            var tracker = new TransformManyBlock<LoadingTx, LoadingTx>(
                loadingTx =>
                {
                    // track stats, ignore coinbase
                    if (newChain.Height > 0 && !loadingTx.IsCoinbase)
                    {
                        pendingTxCount += loadingTx.Transaction.Inputs.Length;
                        this.stats.txRateMeasure.Tick();
                        this.stats.inputRateMeasure.Tick(loadingTx.Transaction.Inputs.Length);
                    }

                    if (!rules.BypassPrevTxLoading)
                        return new[] { loadingTx };
                    else
                        return new LoadingTx[0];
                });

            loadingTxes.LinkTo(tracker, new DataflowLinkOptions { PropagateCompletion = true });

            tracker.Completion.ContinueWith(_ =>
            {
                if (newChain.Height > 0)
                    this.stats.pendingTxesTotalAverageMeasure.Tick(pendingTxCount);

                calcUtxoStopwatch.Stop();
                if (newChain.Height > 0)
                    this.stats.calculateUtxoDurationMeasure.Tick(calcUtxoStopwatch.Elapsed);
            });

            return tracker;
        }

        public void RollbackBlock(ChainedHeader chainedHeader, IEnumerable<BlockTx> blockTxes)
        {
            using (var handle = this.storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                chainStateCursor.BeginTransaction();

                // verify storage chain tip matches this chain state builder's chain tip
                if (!(chainStateCursor.ChainTip == null && chain.Height == 0)
                    && (chainStateCursor.ChainTip.Hash != chain.LastBlock.Hash))
                {
                    throw new InvalidOperationException("ChainStateBuilder is out of sync with underlying storage.");
                }

                var newChain = this.chain.ToBuilder().RemoveBlock(chainedHeader).ToImmutable();

                // keep track of the previoux tx output information for all unminted transactions
                // the information is removed and will be needed to enable a replay of the rolled back block
                var unmintedTxes = ImmutableList.CreateBuilder<UnmintedTx>();

                // rollback the utxo
                this.utxoBuilder.RollbackUtxo(chainStateCursor, this.chain, chainedHeader, blockTxes, unmintedTxes);

                // remove the block from the chain
                chainStateCursor.ChainTip = newChain.LastBlock;

                // remove the rollback information
                chainStateCursor.TryRemoveBlockSpentTxes(chainedHeader.Height);

                // store the replay information
                chainStateCursor.TryAddBlockUnmintedTxes(chainedHeader.Hash, unmintedTxes.ToImmutable());

                // commit the chain state
                this.commitLock.DoWrite(() =>
                {
                    chainStateCursor.CommitTransaction();
                    this.chain = newChain;
                });
            }
        }

        public void LogBlockchainProgress()
        {
            this.commitLock.DoWrite(() =>
            {
                using (var handle = this.storageManager.OpenChainStateCursor())
                {
                    var chainStateCursor = handle.Item;

                    chainStateCursor.BeginTransaction(readOnly: true);
                    LogProgressInner(chainStateCursor);
                }
            });
        }

        private void LogProgressInner(IChainStateCursor chainStateCursor)
        {
            if (DateTime.UtcNow - this.Stats.lastLogTime < TimeSpan.FromSeconds(15))
                return;
            else
                this.Stats.lastLogTime = DateTime.UtcNow;

            var elapsedSeconds = this.Stats.durationStopwatch.Elapsed.TotalSeconds;
            var blockRate = this.stats.blockRateMeasure.GetAverage(TimeSpan.FromSeconds(1));
            var txRate = this.stats.txRateMeasure.GetAverage(TimeSpan.FromSeconds(1));
            var inputRate = this.stats.inputRateMeasure.GetAverage(TimeSpan.FromSeconds(1));

            logger.Info(
                string.Join("\n",
                    new string('-', 80),
                    "Height: {0,10} | Duration: {1} /*| Validation: {2} */| Blocks/s: {3,7} | Tx/s: {4,7} | Inputs/s: {5,7} | Processed Tx: {6,7} | Processed Inputs: {7,7} | Utx Size: {8,7} | Utxo Size: {9,7}",
                    new string('-', 80),
                //TODO stats come from CoreStorage, not exposed on ICoreStorage, stats need to be moved
                //"Avg. Prev Tx Load Time: {10,12:#,##0.000}ms",
                //"Prev Tx Load Rate:  {11,12:#,##0}/s",
                //new string('-', 80),
                    "Block Txes Read:            {12,12:N3}ms",
                    "UTXO Look-ahead:            {13,12:N3}ms",
                    "Avg. UTXO Calculation Time: {14,12:#,##0.000}ms",
                    new string('-', 80),
                    "Avg. Prev Txes per Block:                  {15,12:#,##0}",
                    "Avg. Pending Prev Txes at UTXO Completion: {16,12:#,##0}",
                    new string('-', 80),
                    "Avg. UTXO Application Time: {17,12:#,##0.000}ms",
                    "Avg. Wait-to-complete Time: {18,12:#,##0.000}ms",
                    "Avg. UTXO Commit Time:      {19,12:#,##0.000}ms",
                    "Avg. AddBlock Time:         {20,12:#,##0.000}ms",
                    new string('-', 80)
                )
                .Format2
                (
                /*0*/ this.chain.Height.ToString("#,##0"),
                /*1*/ this.Stats.durationStopwatch.Elapsed.ToString(@"hh\:mm\:ss"),
                /*2*/ this.Stats.validateStopwatch.Elapsed.ToString(@"hh\:mm\:ss"),
                /*3*/ blockRate.ToString("#,##0"),
                /*4*/ txRate.ToString("#,##0"),
                /*5*/ inputRate.ToString("#,##0"),
                /*6*/ chainStateCursor.TotalTxCount.ToString("#,##0"),
                /*7*/ chainStateCursor.TotalInputCount.ToString("#,##0"),
                /*8*/ chainStateCursor.UnspentTxCount.ToString("#,##0"),
                /*9*/ chainStateCursor.UnspentOutputCount.ToString("#,##0"),
                //TODO stats come from CoreStorage, not exposed on ICoreStorage, stats need to be moved
                /*10*/ 0, //this.coreStorage.GetTxLoadDuration().TotalMilliseconds,
                /*11*/ 0, //this.coreStorage.GetTxLoadRate(),
                /*12*/ this.Stats.txesReadDurationMeasure.GetAverage().TotalMilliseconds,
                /*13*/ this.Stats.lookAheadDurationMeasure.GetAverage().TotalMilliseconds,
                /*14*/ this.Stats.calculateUtxoDurationMeasure.GetAverage().TotalMilliseconds,
                /*15*/ this.Stats.pendingTxesTotalAverageMeasure.GetAverage(),
                /*16*/ this.Stats.pendingTxesAtCompleteAverageMeasure.GetAverage(),
                /*17*/ this.Stats.applyUtxoDurationMeasure.GetAverage().TotalMilliseconds,
                /*18*/ this.Stats.waitToCompleteDurationMeasure.GetAverage().TotalMilliseconds,
                /*19*/ this.Stats.commitUtxoDurationMeasure.GetAverage().TotalMilliseconds,
                /*20*/ this.Stats.addBlockDurationMeasure.GetAverage().TotalMilliseconds
                ));
        }

        //TODO cache the latest immutable snapshot
        public ChainState ToImmutable()
        {
            return this.commitLock.DoRead(() =>
                new ChainState(this.chain, this.storageManager));
        }

        private bool TryReadChain(IChainStateCursor chainStateCursor, UInt256 blockHash, out Chain chain)
        {
            return Chain.TryReadChain(blockHash, out chain,
                headerHash =>
                {
                    ChainedHeader chainedHeader;
                    chainStateCursor.TryGetHeader(headerHash, out chainedHeader);
                    return chainedHeader;
                });
        }

        public sealed class BuilderStats : IDisposable
        {
            private static readonly TimeSpan sampleCutoff = TimeSpan.FromMinutes(5);
            private static readonly TimeSpan sampleResolution = TimeSpan.FromSeconds(5);

            public Stopwatch durationStopwatch = Stopwatch.StartNew();
            public Stopwatch validateStopwatch = new Stopwatch();

            public readonly DurationMeasure calculateUtxoDurationMeasure = new DurationMeasure(sampleCutoff, sampleResolution);
            public readonly DurationMeasure applyUtxoDurationMeasure = new DurationMeasure(sampleCutoff, sampleResolution);
            public readonly DurationMeasure commitUtxoDurationMeasure = new DurationMeasure(sampleCutoff, sampleResolution);
            public readonly AverageMeasure pendingTxesTotalAverageMeasure = new AverageMeasure(sampleCutoff, sampleResolution);
            public readonly AverageMeasure pendingTxesAtCompleteAverageMeasure = new AverageMeasure(sampleCutoff, sampleResolution);
            public readonly DurationMeasure waitToCompleteDurationMeasure = new DurationMeasure(sampleCutoff, sampleResolution);
            public readonly DurationMeasure addBlockDurationMeasure = new DurationMeasure(sampleCutoff, sampleResolution);
            public readonly RateMeasure blockRateMeasure = new RateMeasure(sampleCutoff, sampleResolution);
            public readonly RateMeasure txRateMeasure = new RateMeasure(sampleCutoff, sampleResolution);
            public readonly RateMeasure inputRateMeasure = new RateMeasure(sampleCutoff, sampleResolution);
            public readonly DurationMeasure txesReadDurationMeasure = new DurationMeasure(TimeSpan.FromMinutes(5), TimeSpan.FromSeconds(5));
            public readonly DurationMeasure lookAheadDurationMeasure = new DurationMeasure(TimeSpan.FromMinutes(5), TimeSpan.FromSeconds(5));

            public DateTime lastLogTime = DateTime.UtcNow;

            internal BuilderStats() { }

            public void Dispose()
            {
                this.calculateUtxoDurationMeasure.Dispose();
                this.applyUtxoDurationMeasure.Dispose();
                this.commitUtxoDurationMeasure.Dispose();
                this.pendingTxesTotalAverageMeasure.Dispose();
                this.pendingTxesAtCompleteAverageMeasure.Dispose();
                this.waitToCompleteDurationMeasure.Dispose();
                this.blockRateMeasure.Dispose();
                this.addBlockDurationMeasure.Dispose();
                this.txRateMeasure.Dispose();
                this.inputRateMeasure.Dispose();
                this.txesReadDurationMeasure.Dispose();
                this.lookAheadDurationMeasure.Dispose();
            }
        }
    }
}