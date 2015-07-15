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

        private readonly ChainStateBuilderStats stats;

        public ChainStateBuilder(IBlockchainRules rules, ICoreStorage coreStorage, IStorageManager storageManager)
        {
            this.rules = rules;
            this.coreStorage = coreStorage;
            this.storageManager = storageManager;

            this.stats = new ChainStateBuilderStats();

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

        public ChainStateBuilderStats Stats { get { return this.stats; } }

        public void AddBlock(ChainedHeader chainedHeader, IEnumerable<Transaction> transactions)
        {
            AddBlock(chainedHeader, transactions.Select((tx, txIndex) => new BlockTx(txIndex, depth: 0, hash: tx.Hash, pruned: false, transaction: tx)));
        }

        public void AddBlock(ChainedHeader chainedHeader, IEnumerable<BlockTx> blockTxes)
        {
            var stopwatch = Stopwatch.StartNew();

            using (var chainState = this.ToImmutable())
            using (var handle = this.storageManager.OpenDeferredChainStateCursor(chainState))
            {
                var chainStateCursor = handle.Item;

                //TODO note - the utxo updates could be applied either while validation is ongoing,
                //TODO        or after it is complete, begin transaction here to apply immediately
                chainStateCursor.BeginTransaction();

                // verify storage chain tip matches this chain state builder's chain tip
                CheckChainTip(chainStateCursor);

                var newChain = this.chain.ToBuilder().AddBlock(chainedHeader).ToImmutable();

                // begin reading block txes into the buffer
                var blockTxesBuffer = new BufferBlock<BlockTx>();
                var sendBlockTxes = blockTxesBuffer.SendAndCompleteAsync(blockTxes);

                // warm-up utxo entries for block txes
                var warmedBlockTxes = UtxoLookAhead.LookAhead(blockTxesBuffer, chainStateCursor);

                // begin calculating the utxo updates
                var loadingTxes = CalculateUtxo(newChain, warmedBlockTxes, chainStateCursor);

                // begin loading txes
                var loadedTxes = TxLoader.LoadTxes(coreStorage, loadingTxes);

                // begin validating the block
                var blockValidator = BlockValidator.ValidateBlock(coreStorage, rules, chainedHeader, loadedTxes);

                // wait for block txes to read
                sendBlockTxes.Wait();

                if (chainedHeader.Height > 0)
                    this.stats.txesReadDurationMeasure.Tick(stopwatch.Elapsed);

                // wait for utxo look-ahead to complete
                warmedBlockTxes.Completion.Wait();

                if (chainedHeader.Height > 0)
                    this.stats.lookAheadDurationMeasure.Tick(stopwatch.Elapsed);

                // wait for utxo calculation
                loadingTxes.Completion.Wait();

                if (chainedHeader.Height > 0)
                    this.stats.calculateUtxoDurationMeasure.Tick(stopwatch.Elapsed);

                // apply the utxo changes, do not yet commit
                chainStateCursor.ChainTip = chainedHeader;
                chainStateCursor.TryAddHeader(chainedHeader);
                chainStateCursor.ApplyChanges();

                if (chainedHeader.Height > 0)
                    this.stats.applyUtxoDurationMeasure.Tick(stopwatch.Elapsed);

                // wait for block validation to complete, any exceptions that ocurred will be thrown
                blockValidator.Wait();

                if (chainedHeader.Height > 0)
                    this.stats.validateDurationMeasure.Tick(stopwatch.Elapsed);

                var totalTxCount = chainStateCursor.TotalTxCount;
                var totalInputCount = chainStateCursor.TotalInputCount;
                var unspentTxCount = chainStateCursor.UnspentTxCount;
                var unspentOutputCount = chainStateCursor.UnspentOutputCount;

                // only commit the utxo changes once block validation has completed
                this.commitLock.DoWrite(() =>
                {
                    chainStateCursor.CommitTransaction();
                    this.chain = newChain;
                });

                if (chainedHeader.Height > 0)
                    this.stats.commitUtxoDurationMeasure.Tick(stopwatch.Elapsed);

                // update total count stats
                stats.Height = chain.Height;
                stats.TotalTxCount = totalTxCount;
                stats.TotalInputCount = totalInputCount;
                stats.UnspentTxCount = unspentTxCount;
                stats.UnspentOutputCount = unspentOutputCount;

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
                CheckChainTip(chainStateCursor);

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

        private void CheckChainTip(IChainStateCursor chainStateCursor)
        {
            var chainTip = chainStateCursor.ChainTip;

            // verify storage chain tip matches this chain state builder's chain tip
            if (!(chainTip == null && chain.Height == -1)
                && (chainTip.Hash != chain.LastBlock.Hash))
            {
                throw new ChainStateOutOfSyncException(chain.LastBlock, chainTip);
            }
        }
    }
}