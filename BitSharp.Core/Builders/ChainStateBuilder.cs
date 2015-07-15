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
using System.Text;
using System.Threading;
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

                //TODO note - the utxo updates could be applied either while validation is ongoing,
                //TODO        or after it is complete, begin transaction here to apply immediately
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
                    {
                        deferredChainStateCursor.CompleteWorkQueue();
                        this.stats.calculateUtxoDurationMeasure.Tick(stopwatch.Elapsed);
                    });

                    // begin loading txes
                    var loadedTxes = TxLoader.LoadTxes(coreStorage, loadingTxes);

                    // begin validating the block
                    var blockValidator = BlockValidator.ValidateBlock(coreStorage, rules, chainedHeader, loadedTxes);

                    // apply the changes, do not yet commit
                    deferredChainStateCursor.ApplyChangesToParent(chainStateCursor);
                    chainStateCursor.ChainTip = chainedHeader;
                    chainStateCursor.TryAddHeader(chainedHeader);

                    if (chainedHeader.Height > 0)
                        this.stats.applyUtxoDurationMeasure.Tick(stopwatch.Elapsed);

                    // wait for block validation to complete, any exceptions that ocurred will be thrown
                    sendBlockTxes.Wait();
                    blockTxesBuffer.Completion.Wait();
                    warmedBlockTxes.Completion.Wait();
                    loadingTxes.Completion.Wait();
                    loadedTxes.Completion.Wait();
                    blockValidator.Wait();

                    if (chainedHeader.Height > 0)
                        this.stats.validateDurationMeasure.Tick(stopwatch.Elapsed);
                }

                // only commit the utxo changes once block validation has completed
                this.commitLock.DoWrite(() =>
                {
                    chainStateCursor.CommitTransaction();
                    this.chain = newChain;
                });

                if (chainedHeader.Height > 0)
                    this.stats.commitUtxoDurationMeasure.Tick(stopwatch.Elapsed);

                // update total count stats
                chainStateCursor.BeginTransaction(readOnly: true);
                stats.Height = chain.Height;
                stats.TotalTxCount = chainStateCursor.TotalTxCount;
                stats.TotalInputCount = chainStateCursor.TotalInputCount;
                stats.UnspentTxCount = chainStateCursor.UnspentTxCount;
                stats.UnspentOutputCount = chainStateCursor.UnspentOutputCount;
                chainStateCursor.RollbackTransaction();

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
            // calculate the new block utxo, only output availability is checked and updated
            var loadingTxes = this.utxoBuilder.CalculateUtxo(chainStateCursor, newChain, blockTxes);

            var tracker = new TransformManyBlock<LoadingTx, LoadingTx>(
                loadingTx =>
                {
                    // track stats, ignore coinbase
                    if (newChain.Height > 0 && !loadingTx.IsCoinbase)
                    {
                        this.stats.txRateMeasure.Tick();
                        this.stats.inputRateMeasure.Tick(loadingTx.Transaction.Inputs.Length);
                    }

                    if (!rules.BypassPrevTxLoading)
                        return new[] { loadingTx };
                    else
                        return new LoadingTx[0];
                });

            loadingTxes.LinkTo(tracker, new DataflowLinkOptions { PropagateCompletion = true });

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
            Throttler.IfElapsed(TimeSpan.FromSeconds(15), () =>
                logger.Info(this.stats.ToString()));
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

            internal Stopwatch durationStopwatch = Stopwatch.StartNew();

            public int Height { get; internal set; }
            public int TotalTxCount { get; internal set; }
            public int TotalInputCount { get; internal set; }
            public int UnspentTxCount { get; internal set; }
            public int UnspentOutputCount { get; internal set; }

            internal readonly RateMeasure blockRateMeasure = new RateMeasure(sampleCutoff, TimeSpan.FromSeconds(1));
            internal readonly RateMeasure txRateMeasure = new RateMeasure(sampleCutoff, TimeSpan.FromSeconds(1));
            internal readonly RateMeasure inputRateMeasure = new RateMeasure(sampleCutoff, TimeSpan.FromSeconds(1));

            internal readonly DurationMeasure txesReadDurationMeasure = new DurationMeasure(sampleCutoff, sampleResolution);
            internal readonly DurationMeasure lookAheadDurationMeasure = new DurationMeasure(sampleCutoff, sampleResolution);
            internal readonly DurationMeasure calculateUtxoDurationMeasure = new DurationMeasure(sampleCutoff, sampleResolution);
            internal readonly DurationMeasure applyUtxoDurationMeasure = new DurationMeasure(sampleCutoff, sampleResolution);
            internal readonly DurationMeasure validateDurationMeasure = new DurationMeasure(sampleCutoff, sampleResolution);
            internal readonly DurationMeasure commitUtxoDurationMeasure = new DurationMeasure(sampleCutoff, sampleResolution);
            internal readonly DurationMeasure addBlockDurationMeasure = new DurationMeasure(sampleCutoff, sampleResolution);

            internal BuilderStats() { }

            public void Dispose()
            {
                this.blockRateMeasure.Dispose();
                this.txRateMeasure.Dispose();
                this.inputRateMeasure.Dispose();

                this.txesReadDurationMeasure.Dispose();
                this.lookAheadDurationMeasure.Dispose();
                this.calculateUtxoDurationMeasure.Dispose();
                this.applyUtxoDurationMeasure.Dispose();
                this.validateDurationMeasure.Dispose();
                this.commitUtxoDurationMeasure.Dispose();
                this.addBlockDurationMeasure.Dispose();
            }

            public override string ToString()
            {
                var statString = new StringBuilder();

                statString.AppendLine("Chain State Builder Stats");
                statString.AppendLine("-------------------------");
                statString.AppendLine("Height:           {0,15:N0}".Format2(Height));
                statString.AppendLine("Duration:         {0,15}".Format2(
                    "{0:#,#00}:{1:mm':'ss}".Format2(durationStopwatch.Elapsed.TotalHours, durationStopwatch.Elapsed)));
                statString.AppendLine("-------------------------");
                statString.AppendLine("Blocks Rate:      {0,15:N0}/s".Format2(blockRateMeasure.GetAverage()));
                statString.AppendLine("Tx Rate:          {0,15:N0}/s".Format2(txRateMeasure.GetAverage()));
                statString.AppendLine("Input Rate:       {0,15:N0}/s".Format2(inputRateMeasure.GetAverage()));
                statString.AppendLine("-------------------------");
                statString.AppendLine("Processed Txes:   {0,15:N0}".Format2(TotalTxCount));
                statString.AppendLine("Processed Inputs: {0,15:N0}".Format2(TotalInputCount));
                statString.AppendLine("Utx Size:         {0,15:N0}".Format2(UnspentTxCount));
                statString.AppendLine("Utxo Size:        {0,15:N0}".Format2(UnspentOutputCount));
                statString.AppendLine("-------------------------");

                var texReadDuration = txesReadDurationMeasure.GetAverage();
                var lookAheadDuration = lookAheadDurationMeasure.GetAverage();
                var calculateUtxoDuration = calculateUtxoDurationMeasure.GetAverage();
                var applyUtxoDuration = applyUtxoDurationMeasure.GetAverage();
                var validateDuration = validateDurationMeasure.GetAverage();
                var commitUtxoDuration = commitUtxoDurationMeasure.GetAverage();
                var addBlockDuration = addBlockDurationMeasure.GetAverage();

                statString.AppendLine(GetPipelineStat("Block Txes Read", texReadDuration, TimeSpan.Zero));
                statString.AppendLine(GetPipelineStat("UTXO Look-ahead", lookAheadDuration, texReadDuration));
                statString.AppendLine(GetPipelineStat("UTXO Calculation", calculateUtxoDuration, lookAheadDuration));
                statString.AppendLine(GetPipelineStat("UTXO Application", applyUtxoDuration, calculateUtxoDuration));
                statString.AppendLine(GetPipelineStat("Block Validation", validateDuration, applyUtxoDuration));
                statString.AppendLine(GetPipelineStat("UTXO Commit", commitUtxoDuration, validateDuration));
                statString.Append(GetPipelineStat("AddBlock Total", addBlockDuration, null));

                return statString.ToString();
            }

            private string GetPipelineStat(string name, TimeSpan duration, TimeSpan? prevDuration)
            {
                var format = "{0,-20} Completion: {1,12:N3}ms";

                TimeSpan delta;
                if (prevDuration != null)
                {
                    format += ", Delta: {2,12:N3}ms";
                    delta = duration - prevDuration.Value;
                }
                else
                    delta = TimeSpan.Zero;

                return string.Format(format, name + ":", duration.TotalMilliseconds, delta.TotalMilliseconds);
            }
        }
    }
}