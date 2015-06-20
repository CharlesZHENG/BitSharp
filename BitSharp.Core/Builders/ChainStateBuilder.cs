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

namespace BitSharp.Core.Builders
{
    internal class ChainStateBuilder : IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly IBlockchainRules rules;
        private readonly CoreStorage coreStorage;
        private readonly IStorageManager storageManager;

        private readonly BlockValidator blockValidator;

        private bool inTransaction;
        private readonly DisposeHandle<IChainStateCursor> chainStateCursorHandle;
        private readonly IChainStateCursor chainStateCursor;
        private ChainBuilder chain;
        private Chain rollbackChain;
        private readonly UtxoBuilder utxoBuilder;

        private readonly UtxoLookAhead utxoLookAhead;

        private readonly ReaderWriterLockSlim commitLock;

        private readonly BuilderStats stats;

        public ChainStateBuilder(IBlockchainRules rules, CoreStorage coreStorage)
        {
            this.rules = rules;
            this.coreStorage = coreStorage;
            this.storageManager = coreStorage.StorageManager;

            this.stats = new BuilderStats();
            this.blockValidator = new BlockValidator(this.stats, this.coreStorage, this.rules);

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

            this.utxoLookAhead = new UtxoLookAhead(this.stats);

            this.commitLock = new ReaderWriterLockSlim();
        }

        public void Dispose()
        {
            this.blockValidator.Dispose();
            this.chainStateCursorHandle.Dispose();
            this.stats.Dispose();
            this.utxoLookAhead.Dispose();
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
                this.BeginTransaction(readOnly: true);
                try
                {
                    this.blockValidator.ValidateTransactions(chainedHeader,
                        loadingTxes =>
                        {
                            using (var deferredChainStateCursor = new DeferredChainStateCursor(this.chainStateCursor))
                            {
                                this.stats.calculateUtxoDurationMeasure.Measure(() =>
                                {
                                    using (var chainState = new ChainState(this.chain.ToImmutable(), this.storageManager))
                                    {
                                        // add the block to the chain
                                        this.chain.AddBlock(chainedHeader);

                                        // calculate the new block utxo, only output availability is checked and updated
                                        var pendingTxCount = 0;
                                        var blockTxesCalculateQueue = this.utxoLookAhead.LookAhead(blockTxes, deferredChainStateCursor);
                                        foreach (var loadingTx in this.utxoBuilder.CalculateUtxo(deferredChainStateCursor, this.chain.ToImmutable(), blockTxesCalculateQueue))
                                        {
                                            if (!rules.BypassPrevTxLoading)
                                                loadingTxes.Add(loadingTx);

                                            // track stats, ignore coinbase
                                            if (!loadingTx.IsCoinbase)
                                            {
                                                pendingTxCount += loadingTx.Transaction.Inputs.Length;
                                                this.stats.txCount++;
                                                this.stats.inputCount += loadingTx.Transaction.Inputs.Length;
                                                this.stats.txRateMeasure.Tick();
                                                this.stats.inputRateMeasure.Tick(loadingTx.Transaction.Inputs.Length);
                                            }
                                        }

                                        this.stats.pendingTxesTotalAverageMeasure.Tick(pendingTxCount);
                                    }
                                });

                                this.stats.applyUtxoDurationMeasure.Measure(() =>
                                {
                                    // switch to a writeable transaction
                                    //TODO doesn't account for rollback/begin exceptions
                                    this.chainStateCursor.RollbackTransaction();
                                    this.chainStateCursor.BeginTransaction();

                                    deferredChainStateCursor.ApplyChangesToParent();
                                    this.chainStateCursor.AddChainedHeader(chainedHeader);
                                });
                            }

                            // finished queuing up block's txes
                            loadingTxes.CompleteAdding();

                            // track stats
                            this.stats.blockCount++;
                        });

                    // commit the chain state
                    this.CommitTransaction();
                }
                catch (Exception)
                {
                    // rollback the chain state on error
                    this.RollbackTransaction();
                    throw;
                }

                // MEASURE: Block Rate
                this.stats.blockRateMeasure.Tick();

                // blockchain processing statistics
                this.Stats.blockCount++;
            });

            this.LogBlockchainProgress();
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
            if (DateTime.UtcNow - this.Stats.lastLogTime < TimeSpan.FromSeconds(15))
                return;
            else
                this.Stats.lastLogTime = DateTime.UtcNow;

            this.commitLock.DoWrite(() =>
            {
                this.BeginTransaction(readOnly: true);
                try
                {
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
                            "UTXO Read Rate:             {17,12:#,##0}/s",
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
                        /*10*/ this.Stats.prevTxLoadDurationMeasure.GetAverage().TotalMilliseconds,
                        /*11*/ this.Stats.prevTxLoadRateMeasure.GetAverage(),
                        /*12*/ this.Stats.pendingTxesTotalAverageMeasure.GetAverage(),
                        /*13*/ this.Stats.pendingTxesAtCompleteAverageMeasure.GetAverage(),
                        /*14*/ this.Stats.calculateUtxoDurationMeasure.GetAverage().TotalMilliseconds,
                        /*15*/ this.Stats.applyUtxoDurationMeasure.GetAverage().TotalMilliseconds,
                        /*16*/ this.Stats.waitToCompleteDurationMeasure.GetAverage().TotalMilliseconds,
                        /*17*/ this.Stats.utxoReadRateMeasure.GetAverage()
                        ));
                }
                finally
                {
                    this.RollbackTransaction();
                }
            });
        }

        //TODO cache the latest immutable snapshot
        public ChainState ToImmutable()
        {
            return this.commitLock.DoRead(() =>
                new ChainState(this.chain.ToImmutable(), this.storageManager));
        }

        private void BeginTransaction(bool readOnly = false)
        {
            if (this.inTransaction)
                throw new InvalidOperationException();

            this.chainStateCursor.BeginTransaction(readOnly);
            this.rollbackChain = this.chain.ToImmutable();
            this.inTransaction = true;
        }

        private void CommitTransaction()
        {
            if (!this.inTransaction)
                throw new InvalidOperationException();

            this.chainStateCursor.CommitTransaction();
            this.rollbackChain = null;
            this.inTransaction = false;
        }

        private void RollbackTransaction()
        {
            if (!this.inTransaction)
                throw new InvalidOperationException();

            this.chainStateCursor.RollbackTransaction();
            this.chain = this.rollbackChain.ToBuilder();
            this.inTransaction = false;
        }

        public sealed class BuilderStats : IDisposable
        {
            private static readonly TimeSpan sampleCutoff = TimeSpan.FromMinutes(5);
            private static readonly TimeSpan sampleResolution = TimeSpan.FromSeconds(5);

            public Stopwatch durationStopwatch = Stopwatch.StartNew();
            public Stopwatch validateStopwatch = new Stopwatch();

            public long blockCount;
            public long txCount;
            public long inputCount;

            public readonly DurationMeasure calculateUtxoDurationMeasure = new DurationMeasure(sampleCutoff, sampleResolution);
            public readonly DurationMeasure applyUtxoDurationMeasure = new DurationMeasure(sampleCutoff, sampleResolution);
            public readonly DurationMeasure prevTxLoadDurationMeasure = new DurationMeasure(sampleCutoff, sampleResolution);
            public readonly RateMeasure prevTxLoadRateMeasure = new RateMeasure(sampleCutoff, sampleResolution);
            public readonly AverageMeasure pendingTxesTotalAverageMeasure = new AverageMeasure(sampleCutoff, sampleResolution);
            public readonly AverageMeasure pendingTxesAtCompleteAverageMeasure = new AverageMeasure(sampleCutoff, sampleResolution);
            public readonly DurationMeasure waitToCompleteDurationMeasure = new DurationMeasure(sampleCutoff, sampleResolution);
            public readonly RateMeasure blockRateMeasure = new RateMeasure(sampleCutoff, sampleResolution);
            public readonly RateMeasure txRateMeasure = new RateMeasure(sampleCutoff, sampleResolution);
            public readonly RateMeasure inputRateMeasure = new RateMeasure(sampleCutoff, sampleResolution);
            public readonly RateMeasure utxoReadRateMeasure = new RateMeasure(sampleCutoff, sampleResolution);

            public DateTime lastLogTime = DateTime.UtcNow;

            internal BuilderStats() { }

            public void Dispose()
            {
                this.calculateUtxoDurationMeasure.Dispose();
                this.applyUtxoDurationMeasure.Dispose();
                this.prevTxLoadDurationMeasure.Dispose();
                this.prevTxLoadRateMeasure.Dispose();
                this.pendingTxesTotalAverageMeasure.Dispose();
                this.pendingTxesAtCompleteAverageMeasure.Dispose();
                this.waitToCompleteDurationMeasure.Dispose();
                this.blockRateMeasure.Dispose();
                this.txRateMeasure.Dispose();
                this.inputRateMeasure.Dispose();
                this.utxoReadRateMeasure.Dispose();
            }
        }
    }
}