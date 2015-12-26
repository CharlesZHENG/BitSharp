using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage;
using NLog;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks.Dataflow;

namespace BitSharp.Core.Builders
{
    internal static class UtxoReplayer
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        public static ISourceBlock<ValidatableTx> ReplayCalculateUtxo(ICoreStorage coreStorage, IChainState chainState, ChainedHeader replayBlock, CancellationToken cancelToken = default(CancellationToken))
        {
            return ReplayFromTxIndex(coreStorage, chainState, replayBlock, replayForward: true, cancelToken: cancelToken);
        }

        public static ISourceBlock<ValidatableTx> ReplayRollbackUtxo(ICoreStorage coreStorage, IChainState chainState, ChainedHeader replayBlock, CancellationToken cancelToken = default(CancellationToken))
        {
            // replaying rollback of an on-chain block, use the chainstate tx index for replay, same as replaying forward
            if (chainState.Chain.BlocksByHash.ContainsKey(replayBlock.Hash))
            {
                return ReplayFromTxIndex(coreStorage, chainState, replayBlock, replayForward: false, cancelToken: cancelToken);
            }
            // replaying rollback of an off-chain (re-org) block, use the unminted information for replay
            else
            {
                IImmutableList<UnmintedTx> unmintedTxesList;
                if (!chainState.TryGetBlockUnmintedTxes(replayBlock.Hash, out unmintedTxesList))
                {
                    //TODO if a wallet/monitor were to see a chainstate block that wasn't flushed to disk yet,
                    //TODO and if bitsharp crashed, and if the block was orphaned: then the orphaned block would
                    //TODO not be present in the chainstate, and it would not get rolled back to generate unminted information.
                    //TODO DeferredChainStateCursor should be used in order to re-org the chainstate in memory and calculate the unminted information
                    throw new MissingDataException(replayBlock.Hash);
                }

                var unmintedTxes = ImmutableDictionary.CreateRange(
                    unmintedTxesList.Select(x => new KeyValuePair<UInt256, UnmintedTx>(x.TxHash, x)));

                var lookupLoadingTx = new TransformBlock<DecodedBlockTx, ValidatableTx>(
                    blockTx =>
                    {
                        var tx = blockTx.Transaction;
                        var txIndex = blockTx.Index;
                        var prevTxOutputs = ImmutableArray.CreateBuilder<TxOutput>(!blockTx.IsCoinbase ? tx.Inputs.Length : 0);

                        if (!blockTx.IsCoinbase)
                        {
                            UnmintedTx unmintedTx;
                            if (!unmintedTxes.TryGetValue(tx.Hash, out unmintedTx))
                                throw new MissingDataException(replayBlock.Hash);

                            prevTxOutputs.AddRange(unmintedTx.PrevTxOutputs);
                        }

                        return new ValidatableTx(blockTx, replayBlock, prevTxOutputs.MoveToImmutable());
                    });

                IEnumerator<BlockTx> blockTxes;
                if (!coreStorage.TryReadBlockTransactions(replayBlock.Hash, out blockTxes))
                {
                    throw new MissingDataException(replayBlock.Hash);
                }

                var blockTxesBuffer = new BufferBlock<DecodedBlockTx>();
                blockTxesBuffer.LinkTo(lookupLoadingTx, new DataflowLinkOptions { PropagateCompletion = true });

                blockTxesBuffer.SendAndCompleteAsync(blockTxes.UsingAsEnumerable().Select(x => x.Decode()).Reverse(), cancelToken).Forget();

                return lookupLoadingTx;
            }
        }

        private static ISourceBlock<ValidatableTx> ReplayFromTxIndex(ICoreStorage coreStorage, IChainState chainState, ChainedHeader replayBlock, bool replayForward, CancellationToken cancelToken = default(CancellationToken))
        {
            //TODO use replayForward to retrieve blocks in reverse order
            //TODO also check that the block hasn't been pruned (that information isn't stored yet)

            IEnumerator<BlockTx> blockTxes;
            if (!coreStorage.TryReadBlockTransactions(replayBlock.Hash, out blockTxes))
            {
                throw new MissingDataException(replayBlock.Hash);
            }

            var blockTxesBuffer = new BufferBlock<DecodedBlockTx>();
            if (replayForward)
                blockTxesBuffer.SendAndCompleteAsync(blockTxes.UsingAsEnumerable().Select(x => x.Decode()), cancelToken).Forget();
            else
                blockTxesBuffer.SendAndCompleteAsync(blockTxes.UsingAsEnumerable().Select(x => x.Decode()).Reverse(), cancelToken).Forget();

            // begin looking up txes
            var lookupValidatableTx = InitLookupValidatableTx(chainState, replayBlock, cancelToken);
            blockTxesBuffer.LinkTo(lookupValidatableTx, new DataflowLinkOptions { PropagateCompletion = true });

            return lookupValidatableTx;
        }

        //TODO this should lookup in parallel and then get sorted, like TxLoader, etc.
        private static TransformBlock<DecodedBlockTx, ValidatableTx> InitLookupValidatableTx(IChainState chainState, ChainedHeader replayBlock, CancellationToken cancelToken)
        {
            return new TransformBlock<DecodedBlockTx, ValidatableTx>(
                blockTx =>
                {
                    var tx = blockTx.Transaction;
                    var txIndex = blockTx.Index;

                    var prevTxOutputs = ImmutableArray.CreateBuilder<TxOutput>(!blockTx.IsCoinbase ? tx.Inputs.Length : 0);

                    if (!blockTx.IsCoinbase)
                    {
                        for (var inputIndex = 0; inputIndex < tx.Inputs.Length; inputIndex++)
                        {
                            var input = tx.Inputs[inputIndex];

                            UnspentTx unspentTx;
                            if (!chainState.TryGetUnspentTx(input.PreviousTxOutputKey.TxHash, out unspentTx))
                                throw new MissingDataException(replayBlock.Hash);

                            prevTxOutputs.Add(unspentTx.TxOutputs[(int)input.PreviousTxOutputKey.TxOutputIndex]);
                        }
                    }

                    return new ValidatableTx(blockTx, replayBlock, prevTxOutputs.MoveToImmutable());
                },
                new ExecutionDataflowBlockOptions { CancellationToken = cancelToken, MaxDegreeOfParallelism = Math.Min(Environment.ProcessorCount, chainState.CursorCount) });
        }
    }
}
