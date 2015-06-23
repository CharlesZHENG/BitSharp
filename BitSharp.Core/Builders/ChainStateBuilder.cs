using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Reactive.Linq;
using System.Reactive;
using System.Reactive.Disposables;
using System.Threading.Tasks;

namespace BitSharp.Core.Builders
{
    internal class ChainStateBuilder : IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly IBlockchainRules rules;
        private readonly CoreStorage coreStorage;
        private readonly IStorageManager storageManager;

        private readonly UtxoLookAhead utxoLookAhead;
        private readonly ParallelReader<LoadingTx> calcUtxoSource;
        private readonly TxLoader txLoader;
        private readonly BlockValidator blockValidator;

        private bool inTransaction;
        private readonly DisposeHandle<IChainStateCursor> chainStateCursorHandle;
        private readonly IChainStateCursor chainStateCursor;
        private ChainBuilder chain;
        private Chain rollbackChain;
        private readonly UtxoBuilder utxoBuilder;

        private readonly ReaderWriterLockSlim commitLock;

        private readonly BuilderStats stats;

        public ChainStateBuilder(IBlockchainRules rules, CoreStorage coreStorage)
        {
            this.rules = rules;
            this.coreStorage = coreStorage;
            this.storageManager = coreStorage.StorageManager;

            this.stats = new BuilderStats();

            this.chainStateCursorHandle = this.storageManager.OpenChainStateCursor();
            this.chainStateCursor = this.chainStateCursorHandle.Item;

            this.chainStateCursor.BeginTransaction(readOnly: true);
            try
            {
                this.chain = new ChainBuilder(chainStateCursor.ReadChain());
            }
            finally
            {
                this.chainStateCursor.RollbackTransaction();
            }
            this.utxoBuilder = new UtxoBuilder();

            // thread count for i/o task (TxLoader)
            var ioThreadCount = Environment.ProcessorCount * 2;

            this.utxoLookAhead = new UtxoLookAhead();
            this.calcUtxoSource = new ParallelReader<LoadingTx>("ChainStateBuilder.CalcUtxoSource");
            this.txLoader = new TxLoader("ChainStateBuilder", coreStorage, ioThreadCount);
            this.blockValidator = new BlockValidator(this.coreStorage, this.rules);

            this.commitLock = new ReaderWriterLockSlim();
        }

        public void Dispose()
        {
            this.utxoLookAhead.Dispose();
            this.calcUtxoSource.Dispose();
            this.txLoader.Dispose();
            this.blockValidator.Dispose();
            this.chainStateCursorHandle.Dispose();
            this.stats.Dispose();
            this.commitLock.Dispose();
        }

        public Chain Chain
        {
            get
            {
                return this.commitLock.DoRead(() =>
                    this.chain.ToImmutable());
            }
        }

        public BuilderStats Stats { get { return this.stats; } }

        public void AddBlock(ChainedHeader chainedHeader, IEnumerable<Transaction> transactions)
        {
            AddBlock(chainedHeader, transactions.Select((tx, txIndex) => new BlockTx(txIndex, depth: 0, hash: tx.Hash, pruned: false, transaction: tx)));
        }

        public void AddBlock(ChainedHeader chainedHeader, IEnumerable<BlockTx> blockTxes)
        {
            this.commitLock.DoWrite(() =>
            {
                var stopwatch = Stopwatch.StartNew();
                var committed = false;
                this.BeginTransaction(readOnly: true, onCursor: false);
                try
                {
                    DeferredChainStateCursor deferredChainStateCursor;
                    this.chainStateCursor.BeginTransaction(readOnly: true);
                    try
                    {
                        deferredChainStateCursor = new DeferredChainStateCursor(this.chainStateCursor);
                    }
                    finally
                    {
                        this.chainStateCursor.RollbackTransaction();
                    }

                    var cursorInTransaction = false;
                    try
                    {
                        Task calcUtxoReadsQueueTask;
                        var blockTxesCalculateQueue = this.utxoLookAhead.LookAhead(blockTxes, deferredChainStateCursor);
                        using (var calcUtxoTask = this.calcUtxoSource.ReadAsync(CalculateUtxo(chainedHeader, blockTxesCalculateQueue, deferredChainStateCursor), null, null, out calcUtxoReadsQueueTask).WaitOnDispose())
                        using (var loadedTxesTask = this.txLoader.LoadTxes(calcUtxoSource).WaitOnDispose())
                        using (var blockValidationTask = this.blockValidator.ValidateTransactions(chainedHeader, txLoader).WaitOnDispose())
                        {
                            // wait for the utxo calculation to finish before applying
                            calcUtxoReadsQueueTask.Wait();

                            //TODO note - the utxo updates could be applied either while validation is ongoing, or after it is complete
                            this.stats.applyUtxoDurationMeasure.MeasureIf(chainedHeader.Height > 0, () =>
                            {
                                // begin the transaction to apply the utxo changes
                                this.chainStateCursor.BeginTransaction();
                                cursorInTransaction = true;

                                // apply the changes, do not yet commit
                                deferredChainStateCursor.ApplyChangesToParent();
                                this.chainStateCursor.AddChainedHeader(chainedHeader);
                            });

                            // wait for block validation to complete, any exceptions that ocurred will be thrown
                            this.stats.waitToCompleteDurationMeasure.MeasureIf(chainedHeader.Height > 0, () =>
                            {
                                blockValidationTask.Dispose();
                            });
                        }

                        this.stats.commitUtxoDurationMeasure.MeasureIf(chainedHeader.Height > 0, () =>
                        {
                            // only commit the utxo changes once block validation has completed
                            Debug.Assert(cursorInTransaction);
                            this.chainStateCursor.CommitTransaction();
                            cursorInTransaction = false;

                            // commit the chain state
                            this.CommitTransaction(onCursor: false);
                            committed = true;
                        });
                        
                        // MEASURE: Block Rate
                        if (chainedHeader.Height > 0)
                            this.stats.blockRateMeasure.Tick();
                    }
                    finally
                    {
                        // rollback if the transaction was not comitted
                        if (cursorInTransaction)
                            this.chainStateCursor.RollbackTransaction();
                    }
                }
                finally
                {
                    stats.addBlockDurationMeasure.Tick(stopwatch.Elapsed);
                    // rollback the chain state on error
                    if (!committed)
                        this.RollbackTransaction(onCursor: false);
                }
            });

            this.LogBlockchainProgress();
        }

        private IEnumerable<LoadingTx> CalculateUtxo(ChainedHeader chainedHeader, IEnumerable<BlockTx> blockTxes, IChainStateCursor chainStateCursor)
        {
            this.chainStateCursor.BeginTransaction(readOnly: true);
            try
            {
                var calcUtxoStopwatch = Stopwatch.StartNew();
                using (var chainState = new ChainState(this.chain.ToImmutable(), this.storageManager))
                {
                    // add the block to the chain
                    this.chain.AddBlock(chainedHeader);

                    // calculate the new block utxo, only output availability is checked and updated
                    var pendingTxCount = 0;
                    foreach (var loadingTx in this.utxoBuilder.CalculateUtxo(chainStateCursor, this.chain.ToImmutable(), blockTxes))
                    {
                        if (!rules.BypassPrevTxLoading)
                            yield return loadingTx;

                        // track stats, ignore coinbase
                        if (chainedHeader.Height > 0 && !loadingTx.IsCoinbase)
                        {
                            pendingTxCount += loadingTx.Transaction.Inputs.Length;
                            this.stats.txRateMeasure.Tick();
                            this.stats.inputRateMeasure.Tick(loadingTx.Transaction.Inputs.Length);
                        }

                        if (calcUtxoStopwatch.Elapsed > TimeSpan.FromSeconds(15))
                            this.LogProgressInner();
                    }

                    if (chainedHeader.Height > 0)
                        this.stats.pendingTxesTotalAverageMeasure.Tick(pendingTxCount);
                }

                calcUtxoStopwatch.Stop();
                if (chainedHeader.Height > 0)
                {
                    this.stats.calculateUtxoDurationMeasure.Tick(calcUtxoStopwatch.Elapsed);
                    this.stats.pendingTxesAtCompleteAverageMeasure.Tick(this.txLoader.Count);
                }
            }
            finally
            {
                this.chainStateCursor.RollbackTransaction();
            }
        }

        public void RollbackBlock(ChainedHeader chainedHeader, IEnumerable<BlockTx> blockTxes)
        {
            this.commitLock.DoWrite(() =>
            {
                this.BeginTransaction();
                try
                {
                    // keep track of the previoux tx output information for all unminted transactions
                    // the information is removed and will be needed to enable a replay of the rolled back block
                    var unmintedTxes = ImmutableList.CreateBuilder<UnmintedTx>();

                    // rollback the utxo
                    this.utxoBuilder.RollbackUtxo(this.chainStateCursor, this.chain.ToImmutable(), chainedHeader, blockTxes, unmintedTxes);

                    // remove the block from the chain
                    this.chain.RemoveBlock(chainedHeader);
                    this.chainStateCursor.RemoveChainedHeader(chainedHeader);

                    // remove the rollback information
                    this.chainStateCursor.TryRemoveBlockSpentTxes(chainedHeader.Height);

                    // store the replay information
                    this.chainStateCursor.TryAddBlockUnmintedTxes(chainedHeader.Hash, unmintedTxes.ToImmutable());

                    // commit the chain state
                    this.CommitTransaction();
                }
                catch (Exception)
                {
                    this.RollbackTransaction();
                    throw;
                }
            });
        }

        public void LogBlockchainProgress()
        {
            this.commitLock.DoWrite(() =>
            {
                this.BeginTransaction(readOnly: true);
                try
                {
                    LogProgressInner();
                }
                finally
                {
                    this.RollbackTransaction();
                }
            });
        }

        private void LogProgressInner()
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
                    "Avg. Prev Tx Load Time: {10,12:#,##0.000}ms",
                    "Prev Tx Load Rate:  {11,12:#,##0}/s",
                    new string('-', 80),
                    "Avg. Prev Txes per Block:                  {12,12:#,##0}",
                    "Avg. Pending Prev Txes at UTXO Completion: {13,12:#,##0}",
                    new string('-', 80),
                    "Avg. UTXO Calculation Time: {14,12:#,##0.000}ms",
                    "Avg. UTXO Application Time: {15,12:#,##0.000}ms",
                    "Avg. Wait-to-complete Time: {16,12:#,##0.000}ms",
                    "Avg. UTXO Commit Time:      {17,12:#,##0.000}ms",
                    "Avg. AddBlock Time:         {18,12:#,##0.000}ms",
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
                /*6*/ this.chainStateCursor.TotalTxCount.ToString("#,##0"),
                /*7*/ this.chainStateCursor.TotalInputCount.ToString("#,##0"),
                /*8*/ this.chainStateCursor.UnspentTxCount.ToString("#,##0"),
                /*9*/ this.chainStateCursor.UnspentOutputCount.ToString("#,##0"),
                /*10*/ this.coreStorage.GetTxLoadDuration().TotalMilliseconds,
                /*11*/ this.coreStorage.GetTxLoadRate(),
                /*12*/ this.Stats.pendingTxesTotalAverageMeasure.GetAverage(),
                /*13*/ this.Stats.pendingTxesAtCompleteAverageMeasure.GetAverage(),
                /*14*/ this.Stats.calculateUtxoDurationMeasure.GetAverage().TotalMilliseconds,
                /*15*/ this.Stats.applyUtxoDurationMeasure.GetAverage().TotalMilliseconds,
                /*16*/ this.Stats.waitToCompleteDurationMeasure.GetAverage().TotalMilliseconds,
                /*17*/ this.Stats.commitUtxoDurationMeasure.GetAverage().TotalMilliseconds,
                /*18*/ this.Stats.addBlockDurationMeasure.GetAverage().TotalMilliseconds
                ));
        }

        //TODO cache the latest immutable snapshot
        public ChainState ToImmutable()
        {
            return this.commitLock.DoRead(() =>
                new ChainState(this.chain.ToImmutable(), this.storageManager));
        }

        private void BeginTransaction(bool readOnly = false, bool onCursor = true)
        {
            if (this.inTransaction)
                throw new InvalidOperationException();

            if (onCursor)
                this.chainStateCursor.BeginTransaction(readOnly);
            this.rollbackChain = this.chain.ToImmutable();
            this.inTransaction = true;
        }

        private void CommitTransaction(bool onCursor = true)
        {
            if (!this.inTransaction)
                throw new InvalidOperationException();

            if (onCursor)
                this.chainStateCursor.CommitTransaction();
            this.rollbackChain = null;
            this.inTransaction = false;
        }

        private void RollbackTransaction(bool onCursor = true)
        {
            if (!this.inTransaction)
                throw new InvalidOperationException();

            if (onCursor)
                this.chainStateCursor.RollbackTransaction();
            this.chain = this.rollbackChain.ToBuilder();
            this.rollbackChain = null;
            this.inTransaction = false;
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
            }
        }
    }
}