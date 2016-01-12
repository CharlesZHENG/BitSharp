using BitSharp.Common;
using BitSharp.Core.Domain;
using NLog;
using System;
using System.Diagnostics;
using System.Linq;
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

        private static TransformManyBlock<DecodedBlockTx, Tuple<TxOutputKey, CompletionCount, DecodedBlockTx>> InitQueueUnspentTxLookup(CancellationToken cancelToken)
        {
            return new TransformManyBlock<DecodedBlockTx, Tuple<TxOutputKey, CompletionCount, DecodedBlockTx>>(
                blockTx =>
                {
                    var tx = blockTx.Transaction;

                    var outputCount = tx.Outputs.Length;
                    var inputCount = !blockTx.IsCoinbase ? tx.Inputs.Length * 2 : 0;

                    var txOutputKeys = new Tuple<TxOutputKey, CompletionCount, DecodedBlockTx>[1 + outputCount + inputCount];
                    var completionCount = new CompletionCount(txOutputKeys.Length);
                    var keyIndex = 0;

                    // warm-up the UnspentTx entry that will be added for the new tx
                    txOutputKeys[keyIndex++] = Tuple.Create(new TxOutputKey(blockTx.Hash, uint.MaxValue), completionCount, blockTx);

                    // warm-up the TxOutput entries that will be added for each of the tx's outputs
                    for (var outputIndex = 0; outputIndex < tx.Outputs.Length; outputIndex++)
                    {
                        var txOutputKey = new TxOutputKey(blockTx.Hash, (uint)outputIndex);
                        txOutputKeys[keyIndex++] = Tuple.Create(txOutputKey, completionCount, blockTx);
                    }

                    // warm-up the previous UnspentTx and TxOutput entries that will be needed for each of the tx's inputs
                    if (!blockTx.IsCoinbase)
                    {
                        for (var inputIndex = 0; inputIndex < tx.Inputs.Length; inputIndex++)
                        {
                            var input = tx.Inputs[inputIndex];

                            // input's previous tx's UnspentTx entry
                            var txOutputKey = new TxOutputKey(input.PrevTxHash, uint.MaxValue);
                            txOutputKeys[keyIndex++] = Tuple.Create(txOutputKey, completionCount, blockTx);

                            // input's previous tx outputs's TxOutput entry
                            txOutputKey = input.PrevTxOutputKey;
                            txOutputKeys[keyIndex++] = Tuple.Create(txOutputKey, completionCount, blockTx);
                        }
                    }

                    Debug.Assert(txOutputKeys.All(x => x != null));

                    return txOutputKeys;
                },
                new ExecutionDataflowBlockOptions { CancellationToken = cancelToken });
        }

        private static TransformManyBlock<Tuple<TxOutputKey, CompletionCount, DecodedBlockTx>, DecodedBlockTx> InitWarmupUtxo(IDeferredChainStateCursor deferredChainStateCursor, CancellationToken cancelToken)
        {
            return new TransformManyBlock<Tuple<TxOutputKey, CompletionCount, DecodedBlockTx>, DecodedBlockTx>(
                tuple =>
                {
                    var txOutputKey = tuple.Item1;
                    var completionCount = tuple.Item2;
                    var blockTx = tuple.Item3;

                    if (txOutputKey.TxOutputIndex == uint.MaxValue)
                        deferredChainStateCursor.WarmUnspentTx(txOutputKey.TxHash);
                    else
                        deferredChainStateCursor.WarmUnspentTxOutput(txOutputKey);

                    if (completionCount.TryComplete())
                        return new[] { blockTx };
                    else
                        return new DecodedBlockTx[0];
                },
                new ExecutionDataflowBlockOptions { CancellationToken = cancelToken, MaxDegreeOfParallelism = Math.Min(Environment.ProcessorCount, deferredChainStateCursor.CursorCount) });
        }
    }
}
