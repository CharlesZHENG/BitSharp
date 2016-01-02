using BitSharp.Common;
using BitSharp.Core.Domain;
using NLog;
using System;
using System.Threading;
using System.Threading.Tasks.Dataflow;

namespace BitSharp.Core.Builders
{
    internal static class UtxoLookAhead
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        public static ISourceBlock<DecodedBlockTx> LookAhead(ISourceBlock<DecodedBlockTx> blockTxes, IDeferredChainStateCursor deferredChainStateCursor, CancellationToken cancelToken = default(CancellationToken))
        {
            // capture the original block txes order
            var orderedBlockTxes = OrderingBlock.CaptureOrder<DecodedBlockTx, DecodedBlockTx, int>(
                blockTxes, blockTx => blockTx.Index, cancelToken);

            // queue each utxo entry to be warmed up: each input's previous transaction, and each new transaction
            var queueUnspentTxLookup = InitQueueUnspentTxLookup(cancelToken);
            orderedBlockTxes.LinkTo(queueUnspentTxLookup, new DataflowLinkOptions { PropagateCompletion = true });

            // warm up each uxto entry
            var warmupUtxo = InitWarmupUtxo(deferredChainStateCursor, cancelToken);
            queueUnspentTxLookup.LinkTo(warmupUtxo, new DataflowLinkOptions { PropagateCompletion = true });

            // return the block txes with warmed utxo entries in original order
            return orderedBlockTxes.ApplyOrder(warmupUtxo, blockTx => blockTx.Index, cancelToken);
        }

        private static TransformManyBlock<DecodedBlockTx, Tuple<UInt256, CompletionCount, DecodedBlockTx>> InitQueueUnspentTxLookup(CancellationToken cancelToken)
        {
            return new TransformManyBlock<DecodedBlockTx, Tuple<UInt256, CompletionCount, DecodedBlockTx>>(
                blockTx =>
                {
                    var tx = blockTx.Transaction;
                    var inputCount = !blockTx.IsCoinbase ? tx.Inputs.Length : 0;
                    var completionCount = new CompletionCount(inputCount + 1);

                    var txHashes = new Tuple<UInt256, CompletionCount, DecodedBlockTx>[inputCount + 1];

                    txHashes[0] = Tuple.Create(blockTx.Hash, completionCount, blockTx);

                    if (!blockTx.IsCoinbase)
                    {
                        for (var inputIndex = 0; inputIndex < tx.Inputs.Length; inputIndex++)
                        {
                            var txHash = tx.Inputs[inputIndex].PrevTxOutputKey.TxHash;
                            txHashes[inputIndex + 1] = Tuple.Create(txHash, completionCount, blockTx);
                        }
                    }

                    return txHashes;
                },
                new ExecutionDataflowBlockOptions { CancellationToken = cancelToken });
        }

        private static TransformManyBlock<Tuple<UInt256, CompletionCount, DecodedBlockTx>, DecodedBlockTx> InitWarmupUtxo(IDeferredChainStateCursor deferredChainStateCursor, CancellationToken cancelToken)
        {
            return new TransformManyBlock<Tuple<UInt256, CompletionCount, DecodedBlockTx>, DecodedBlockTx>(
                tuple =>
                {
                    var txHash = tuple.Item1;
                    var completionCount = tuple.Item2;
                    var blockTx = tuple.Item3;

                    deferredChainStateCursor.WarmUnspentTx(txHash);

                    if (completionCount.TryComplete())
                        return new[] { blockTx };
                    else
                        return new DecodedBlockTx[0];
                },
                new ExecutionDataflowBlockOptions { CancellationToken = cancelToken, MaxDegreeOfParallelism = Math.Min(Environment.ProcessorCount, deferredChainStateCursor.CursorCount) });
        }
    }
}
