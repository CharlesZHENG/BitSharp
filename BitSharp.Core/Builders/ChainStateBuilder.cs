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

        // commitLock is used to ensure that the chain field and underlying storage are always seen in sync
        private readonly ReaderWriterLockSlim commitLock = new ReaderWriterLockSlim();
        private Lazy<Chain> chain;
        private readonly UtxoBuilder utxoBuilder;

        private readonly ChainStateBuilderStats stats;

        public ChainStateBuilder(IBlockchainRules rules, ICoreStorage coreStorage, IStorageManager storageManager)
        {
            this.rules = rules;
            this.coreStorage = coreStorage;
            this.storageManager = storageManager;

            this.chain = new Lazy<Chain>(() => LoadChain());

            this.stats = new ChainStateBuilderStats();
            this.utxoBuilder = new UtxoBuilder();
        }

        public void Dispose()
        {
            stats.Dispose();
            commitLock.Dispose();
        }

        public Chain Chain => chain.Value;

        public ChainStateBuilderStats Stats => stats;

        public async Task AddBlockAsync(ChainedHeader chainedHeader, IEnumerable<BlockTx> blockTxes, CancellationToken cancelToken = default(CancellationToken))
        {
            var stopwatch = Stopwatch.StartNew();
            var txCount = 0;
            var inputCount = 0;

            using (var chainState = ToImmutable())
            using (var handle = storageManager.OpenDeferredChainStateCursor(chainState))
            {
                var chainStateCursor = handle.Item;

                //TODO note - the utxo updates could be applied either while validation is ongoing,
                //TODO        or after it is complete, begin transaction here to apply immediately
                chainStateCursor.BeginTransaction();

                // verify storage chain tip matches this chain state builder's chain tip
                CheckChainTip(chainStateCursor);

                var newChain = chain.Value.ToBuilder().AddBlock(chainedHeader).ToImmutable();

                // begin reading and decoding block txes into the buffer
                var blockTxesBuffer = new BufferBlock<DecodedBlockTx>();
                var sendBlockTxes = blockTxesBuffer.SendAndCompleteAsync(blockTxes.Select(x => x.Decode()), cancelToken);

                // track tx/input stats
                var countBlockTxes = new TransformBlock<DecodedBlockTx, DecodedBlockTx>(
                    blockTx =>
                    {
                        txCount++;
                        inputCount += blockTx.Transaction.Inputs.Length;
                        if (!blockTx.IsCoinbase)
                        {
                            stats.txRateMeasure.Tick();
                            stats.inputRateMeasure.Tick(blockTx.Transaction.Inputs.Length);
                        }

                        return blockTx;
                    });
                blockTxesBuffer.LinkTo(countBlockTxes, new DataflowLinkOptions { PropagateCompletion = true });

                // warm-up utxo entries for block txes
                var warmedBlockTxes = UtxoLookAhead.LookAhead(countBlockTxes, chainStateCursor, cancelToken);

                // begin calculating the utxo updates
                var validatableTxes = utxoBuilder.CalculateUtxo(chainStateCursor, newChain, warmedBlockTxes, cancelToken);

                // begin validating the block
                var blockValidator = BlockValidator.ValidateBlockAsync(coreStorage, rules, newChain, chainedHeader, validatableTxes, cancelToken);

                // prepare to finish applying chain state changes once utxo calculation has completed
                var applyChainState =
                    validatableTxes.Completion.ContinueWith(_ =>
                    {
                        if (validatableTxes.Completion.Status == TaskStatus.RanToCompletion)
                        {
                            // finish applying the utxo changes, do not yet commit
                            chainStateCursor.ChainTip = chainedHeader;
                            chainStateCursor.TryAddHeader(chainedHeader);

                            return chainStateCursor.ApplyChangesAsync();
                        }
                        else
                            return Task.CompletedTask;
                    }).Unwrap();

                var timingTasks = new List<Task>();

                // time block txes read
                timingTasks.Add(
                    blockTxesBuffer.Completion.ContinueWith(_ =>
                    {
                        lock (stopwatch)
                            stats.txesReadDurationMeasure.Tick(stopwatch.Elapsed);
                    }));

                // time utxo look-ahead
                timingTasks.Add(
                    warmedBlockTxes.Completion.ContinueWith(_ =>
                    {
                        lock (stopwatch)
                            stats.lookAheadDurationMeasure.Tick(stopwatch.Elapsed);
                    }));

                // time utxo calculation
                timingTasks.Add(
                    validatableTxes.Completion.ContinueWith(_ =>
                    {
                        lock (stopwatch)
                            stats.calculateUtxoDurationMeasure.Tick(stopwatch.Elapsed);
                    }));

                // time utxo application
                timingTasks.Add(
                    applyChainState.ContinueWith(_ =>
                    {
                        lock (stopwatch)
                            stats.applyUtxoDurationMeasure.Tick(stopwatch.Elapsed);
                    }));

                // time block validation
                timingTasks.Add(
                    blockValidator.ContinueWith(_ =>
                    {
                        lock (stopwatch)
                            stats.validateDurationMeasure.Tick(stopwatch.Elapsed);
                    }));

                // wait for all tasks to complete, any exceptions that ocurred will be thrown
                var pipelineCompletion = PipelineCompletion.Create(
                    new[]
                    {
                        blockTxesBuffer.Completion, sendBlockTxes, countBlockTxes.Completion, warmedBlockTxes.Completion,
                        validatableTxes.Completion, applyChainState, blockValidator
                    }.Concat(timingTasks).ToArray(),
                    new IDataflowBlock[] { blockTxesBuffer, countBlockTxes, warmedBlockTxes, validatableTxes });
                await pipelineCompletion;

                var totalTxCount = chainStateCursor.TotalTxCount;
                var totalInputCount = chainStateCursor.TotalInputCount;
                var unspentTxCount = chainStateCursor.UnspentTxCount;
                var unspentOutputCount = chainStateCursor.UnspentOutputCount;

                // only commit the utxo changes once block validation has completed
                commitLock.DoWrite(() =>
                {
                    chainStateCursor.CommitTransaction();
                    chain = new Lazy<Chain>(() => newChain).Force();
                });
                stats.commitUtxoDurationMeasure.Tick(stopwatch.Elapsed);

                // update total count stats
                stats.Height = chain.Value.Height;
                stats.TotalTxCount = totalTxCount;
                stats.TotalInputCount = totalInputCount;
                stats.UnspentTxCount = unspentTxCount;
                stats.UnspentOutputCount = unspentOutputCount;
            }

            stats.blockRateMeasure.Tick();
            stats.txesPerBlockMeasure.Tick(txCount);
            stats.inputsPerBlockMeasure.Tick(inputCount);

            stats.addBlockDurationMeasure.Tick(stopwatch.Elapsed);

            LogBlockchainProgress();
        }

        private ISourceBlock<T> DropAll<T>(ISourceBlock<T> source)
        {
            var empty = new T[0];
            var dropper = new TransformManyBlock<T, T>(_ => empty);
            source.LinkTo(dropper, new DataflowLinkOptions { PropagateCompletion = true });

            return dropper;
        }

        public void RollbackBlock(ChainedHeader chainedHeader, IEnumerable<BlockTx> blockTxes)
        {
            using (var handle = storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                chainStateCursor.BeginTransaction();

                // verify storage chain tip matches this chain state builder's chain tip
                CheckChainTip(chainStateCursor);

                var newChain = chain.Value.ToBuilder().RemoveBlock(chainedHeader).ToImmutable();

                // keep track of the previoux tx output information for all unminted transactions
                // the information is removed and will be needed to enable a replay of the rolled back block
                var unmintedTxes = ImmutableList.CreateBuilder<UnmintedTx>();

                // rollback the utxo
                utxoBuilder.RollbackUtxo(chainStateCursor, chain.Value, chainedHeader, blockTxes, unmintedTxes);

                // remove the block from the chain
                chainStateCursor.ChainTip = newChain.LastBlock;

                // remove the rollback information
                chainStateCursor.TryRemoveBlockSpentTxes(chainedHeader.Height);

                // store the replay information
                chainStateCursor.TryAddBlockUnmintedTxes(chainedHeader.Hash, unmintedTxes.ToImmutable());

                // commit the chain state
                commitLock.DoWrite(() =>
                {
                    chainStateCursor.CommitTransaction();
                    chain = new Lazy<Chain>(() => newChain).Force();
                });
            }
        }

        public void LogBlockchainProgress()
        {
            Throttler.IfElapsed(TimeSpan.FromSeconds(15), () =>
                logger.Info(stats.ToString()));
        }

        public ChainState ToImmutable()
        {
            return commitLock.DoRead(() =>
                new ChainState(chain.Value, storageManager));
        }

        private Chain LoadChain()
        {
            using (var handle = storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                chainStateCursor.BeginTransaction(readOnly: true);

                var chainTip = chainStateCursor.ChainTip;
                var chainTipHash = chainTip?.Hash;

                Chain chain;
                if (Chain.TryReadChain(chainTipHash, out chain,
                    headerHash =>
                    {
                        ChainedHeader chainedHeader;
                        chainStateCursor.TryGetHeader(headerHash, out chainedHeader);
                        return chainedHeader;
                    }))
                {
                    return chain;
                }
                else
                    throw new StorageCorruptException(StorageType.ChainState, "ChainState is missing header.");
            }
        }

        private void CheckChainTip(IChainStateCursor chainStateCursor)
        {
            var chainTip = chainStateCursor.ChainTip;

            // verify storage chain tip matches this chain state builder's chain tip
            if (!(chainTip == null && chain.Value.Height == -1)
                && (chainTip.Hash != chain.Value.LastBlock.Hash))
            {
                throw new ChainStateOutOfSyncException(chain.Value.LastBlock, chainTip);
            }
        }
    }
}