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

namespace BitSharp.Core.Builders
{
    internal class ChainStateBuilder : IDisposable
    {
        private readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly IBlockchainRules rules;
        private readonly IStorageManager storageManager;

        private readonly BlockValidator blockValidator;

        private bool inTransaction;
        private readonly DisposeHandle<IChainStateCursor> chainStateCursorHandle;
        private readonly IChainStateCursor chainStateCursor;
        private ChainBuilder chain;
        private Chain rollbackChain;
        private readonly UtxoBuilder utxoBuilder;

        private readonly ReaderWriterLockSlim commitLock;

        private readonly BuilderStats stats;

        public ChainStateBuilder(IBlockchainRules rules, IStorageManager storageManager)
        {
            this.rules = rules;
            this.storageManager = storageManager;

            this.stats = new BuilderStats();
            this.blockValidator = new BlockValidator(this.stats, this.storageManager, this.rules);

            this.chainStateCursorHandle = this.storageManager.OpenChainStateCursor();
            this.chainStateCursor = this.chainStateCursorHandle.Item;

            this.chain = new ChainBuilder(chainStateCursor.ReadChain());
            this.utxoBuilder = new UtxoBuilder(chainStateCursor);

            this.commitLock = new ReaderWriterLockSlim();
        }

        public void Dispose()
        {
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
                this.BeginTransaction();
                try
                {
                    using (this.blockValidator.StartValidation(chainedHeader))
                    {
                        // add the block to the chain
                        this.chain.AddBlock(chainedHeader);
                        this.chainStateCursor.AddChainedHeader(chainedHeader);

                        // ignore transactions on geneis block
                        if (chainedHeader.Height > 0)
                        {
                            this.stats.calculateUtxoDurationMeasure.Measure(() =>
                            {
                                // calculate the new block utxo, only output availability is checked and updated
                                var pendingTxCount = 0;
                                foreach (var pendingTx in this.utxoBuilder.CalculateUtxo(this.chain.ToImmutable(), blockTxes.Select(x => x.Transaction)))
                                {
                                    this.blockValidator.AddPendingTx(pendingTx);

                                    // track stats, ignore coinbase
                                    if (pendingTx.TxIndex > 0)
                                    {
                                        pendingTxCount += pendingTx.Transaction.Inputs.Length;
                                        this.stats.txCount++;
                                        this.stats.inputCount += pendingTx.Transaction.Inputs.Length;
                                        this.stats.txRateMeasure.Tick();
                                        this.stats.inputRateMeasure.Tick(pendingTx.Transaction.Inputs.Length);
                                    }
                                }

                                this.stats.pendingTxesTotalAverageMeasure.Tick(pendingTxCount);
                            });
                        }

                        // finished queuing up block's txes
                        this.blockValidator.CompleteAdding();

                        // track stats
                        this.stats.blockCount++;

                        // wait for block validation to complete
                        this.stats.waitToCompleteDurationMeasure.Measure(() =>
                            this.blockValidator.WaitToComplete());

                        // check tx loader results
                        if (this.blockValidator.TxLoaderExceptions.Count > 0)
                        {
                            throw new AggregateException(this.blockValidator.TxLoaderExceptions);
                        }

                        // check tx validation results
                        if (this.blockValidator.TxValidatorExceptions.Count > 0)
                        {
                            throw new AggregateException(this.blockValidator.TxValidatorExceptions);
                        }

                        // check script validation results
                        if (this.blockValidator.ScriptValidatorExceptions.Count > 0)
                        {
                            if (!this.rules.IgnoreScriptErrors)
                                throw new AggregateException(this.blockValidator.ScriptValidatorExceptions);
                            else
                                this.logger.Debug("Ignoring script errors in block: {0,9:#,##0}, errors: {1:#,##0}".Format2(chainedHeader.Height, this.blockValidator.ScriptValidatorExceptions.Count));
                        }
                    }

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

            var logInterval = TimeSpan.FromSeconds(15);
            if (DateTime.UtcNow - this.Stats.lastLogTime >= logInterval)
            {
                this.LogBlockchainProgress();
                this.Stats.lastLogTime = DateTime.UtcNow;
            }
        }

        public void RollbackBlock(ChainedHeader chainedHeader, IEnumerable<BlockTx> blockTxes)
        {
            this.commitLock.DoWrite(() =>
            {
                this.BeginTransaction();
                try
                {
                    // remove the block from the chain
                    this.chain.RemoveBlock(chainedHeader);
                    this.chainStateCursor.RemoveChainedHeader(chainedHeader);

                    // read spent transaction rollback information
                    IImmutableList<SpentTx> spentTxes;
                    if (!this.chainStateCursor.TryGetBlockSpentTxes(chainedHeader.Height, out spentTxes))
                        throw new ValidationException(chainedHeader.Height);

                    //TODO this can be read in reverse instead of doing a dictionary
                    var spentTxesDictionary =
                        ImmutableDictionary.CreateRange(
                            spentTxes.Select(spentTx => new KeyValuePair<UInt256, SpentTx>(spentTx.TxHash, spentTx)));

                    // keep track of the previoux tx output information for all unminted transactions
                    // the information is removed and will be needed to enable a replay of the rolled back block
                    var unmintedTxes = ImmutableList.CreateBuilder<UnmintedTx>();

                    // rollback the utxo
                    this.utxoBuilder.RollbackUtxo(this.chain.ToImmutable(), chainedHeader, blockTxes, spentTxesDictionary, unmintedTxes);

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
            var elapsedSeconds = this.Stats.durationStopwatch.Elapsed.TotalSeconds;
            var blockRate = this.stats.blockRateMeasure.GetAverage(TimeSpan.FromSeconds(1));
            var txRate = this.stats.txRateMeasure.GetAverage(TimeSpan.FromSeconds(1));
            var inputRate = this.stats.inputRateMeasure.GetAverage(TimeSpan.FromSeconds(1));

            this.logger.Info(
                string.Join("\n",
                    new string('-', 80),
                    "Height: {0,10} | Duration: {1} /*| Validation: {2} */| Blocks/s: {3,7} | Tx/s: {4,7} | Inputs/s: {5,7} | Processed Tx: {6,7} | Processed Inputs: {7,7} | Utxo Size: {8,7}",
                    new string('-', 80),
                    "Avg. Prev Tx Load Time: {9,12:#,##0.000}ms",
                    "Prev Tx Load Rate:      {10,12:#,##0.000}/s",
                    new string('-', 80),
                    "Avg. Prev Txes per Block:                  {11,12:#,##0.000}",
                    "Avg. Pending Prev Txes at UTXO Completion: {12,12:#,##0.000}",
                    new string('-', 80),
                    "Avg. UTXO Calculation Time: {13,12:#,##0.000}ms",
                    "Avg. Wait-to-complete Time: {14,12:#,##0.000}ms",
                    new string('-', 80)
                )
                .Format2
                (
                /*0*/ this.Chain.Height.ToString("#,##0"),
                /*1*/ this.Stats.durationStopwatch.Elapsed.ToString(@"hh\:mm\:ss"),
                /*2*/ this.Stats.validateStopwatch.Elapsed.ToString(@"hh\:mm\:ss"),
                /*3*/ blockRate.ToString("#,##0"),
                /*4*/ txRate.ToString("#,##0"),
                /*5*/ inputRate.ToString("#,##0"),
                /*6*/ this.Stats.txCount.ToString("#,##0"),
                /*7*/ this.Stats.inputCount.ToString("#,##0"),
                /*8*/ this.UnspentTxCount.ToString("#,##0"),
                /*9*/ this.Stats.prevTxLoadDurationMeasure.GetAverage().TotalMilliseconds,
                /*10*/ this.Stats.prevTxLoadRateMeasure.GetAverage(),
                /*11*/ this.Stats.pendingTxesTotalAverageMeasure.GetAverage(),
                /*12*/ this.Stats.pendingTxesAtCompleteAverageMeasure.GetAverage(),
                /*13*/ this.Stats.calculateUtxoDurationMeasure.GetAverage().TotalMilliseconds,
                /*14*/ this.Stats.waitToCompleteDurationMeasure.GetAverage().TotalMilliseconds
                ));
        }

        public int UnspentTxCount
        {
            get
            {
                return this.commitLock.DoRead(() =>
                    this.chainStateCursor.UnspentTxCount);
            }
        }

        //TODO cache the latest immutable snapshot
        public ChainState ToImmutable()
        {
            return this.commitLock.DoRead(() =>
                new ChainState(this.chain.ToImmutable(), this.storageManager));
        }

        private void BeginTransaction()
        {
            if (this.inTransaction)
                throw new InvalidOperationException();

            this.chainStateCursor.BeginTransaction();
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
            public readonly DurationMeasure prevTxLoadDurationMeasure = new DurationMeasure(sampleCutoff, sampleResolution);
            public readonly RateMeasure prevTxLoadRateMeasure = new RateMeasure(sampleCutoff, sampleResolution);
            public readonly AverageMeasure pendingTxesTotalAverageMeasure = new AverageMeasure(sampleCutoff, sampleResolution);
            public readonly AverageMeasure pendingTxesAtCompleteAverageMeasure = new AverageMeasure(sampleCutoff, sampleResolution);
            public readonly DurationMeasure waitToCompleteDurationMeasure = new DurationMeasure(sampleCutoff, sampleResolution);
            public readonly RateMeasure blockRateMeasure = new RateMeasure(sampleCutoff, sampleResolution);
            public readonly RateMeasure txRateMeasure = new RateMeasure(sampleCutoff, sampleResolution);
            public readonly RateMeasure inputRateMeasure = new RateMeasure(sampleCutoff, sampleResolution);

            public DateTime lastLogTime = DateTime.UtcNow;

            internal BuilderStats() { }

            public void Dispose()
            {
                this.calculateUtxoDurationMeasure.Dispose();
                this.prevTxLoadDurationMeasure.Dispose();
                this.prevTxLoadRateMeasure.Dispose();
                this.pendingTxesTotalAverageMeasure.Dispose();
                this.pendingTxesAtCompleteAverageMeasure.Dispose();
                this.waitToCompleteDurationMeasure.Dispose();
                this.blockRateMeasure.Dispose();
                this.txRateMeasure.Dispose();
                this.inputRateMeasure.Dispose();
            }
        }
    }
}