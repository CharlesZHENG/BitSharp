using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BitSharp.Core.Builders
{
    internal static class UtxoLookAhead
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private static readonly DurationMeasure txesReadDurationMeasure = new DurationMeasure(TimeSpan.FromMinutes(5), TimeSpan.FromSeconds(5));
        private static readonly DurationMeasure lookAheadDurationMeasure = new DurationMeasure(TimeSpan.FromMinutes(5), TimeSpan.FromSeconds(5));

        public static ISourceBlock<BlockTx> LookAhead(ISourceBlock<BlockTx> blockTxes, IDeferredChainStateCursor deferredChainStateCursor, CancellationToken cancelToken = default(CancellationToken))
        {
            var stopwatch = Stopwatch.StartNew();

            var pendingWarmedTxes = new ConcurrentQueue<Tuple<BlockTx, CompletionCount>>();

            // queue each utxo entry to be warmed up: each input's previous transaction, and each new transaction
            var queueUnspentTxLookup = InitQueueUnspentTxLookup(pendingWarmedTxes, cancelToken);

            // warm up a uxto entry
            var warmupUtxo = InitWarmupUtxo(deferredChainStateCursor, cancelToken);

            // forward any warmed txes, in order
            var forwardWarmedTxes = InitForwardWarmedTxes(pendingWarmedTxes, cancelToken);

            // link notification of a warmed tx to the in-order forwarder
            warmupUtxo.LinkTo(forwardWarmedTxes, new DataflowLinkOptions { PropagateCompletion = true });

            // link the utxo entry queuer to the warmer
            queueUnspentTxLookup.LinkTo(warmupUtxo, new DataflowLinkOptions { PropagateCompletion = true });

            // link the block txes to the unspent tx queuer
            blockTxes.LinkTo(queueUnspentTxLookup, new DataflowLinkOptions { PropagateCompletion = true });

            // track when reading block txes completes
            blockTxes.Completion.ContinueWith(_ =>
            {
                txesReadDurationMeasure.Tick(stopwatch.Elapsed);
                Throttler.IfElapsed(TimeSpan.FromSeconds(5), () =>
                    logger.Info("Block Txes Read: {0,12:N3}ms".Format2(txesReadDurationMeasure.GetAverage().TotalMilliseconds)));
            });

            // track when the overall look ahead completes
            forwardWarmedTxes.Completion.ContinueWith(_ =>
            {
                if (forwardWarmedTxes.Completion.Status == TaskStatus.RanToCompletion)
                    Debug.Assert(pendingWarmedTxes.IsEmpty);

                lookAheadDurationMeasure.Tick(stopwatch.Elapsed);
                Throttler.IfElapsed(TimeSpan.FromSeconds(5), () =>
                    logger.Info("UTXO Look-ahead: {0,12:N3}ms".Format2(lookAheadDurationMeasure.GetAverage().TotalMilliseconds)));
            });

            return forwardWarmedTxes;
        }

        private static TransformManyBlock<BlockTx, Tuple<UInt256, CompletionCount>> InitQueueUnspentTxLookup(ConcurrentQueue<Tuple<BlockTx, CompletionCount>> pendingWarmedTxes, CancellationToken cancelToken)
        {
            return new TransformManyBlock<BlockTx, Tuple<UInt256, CompletionCount>>(
                blockTx =>
                {
                    var inputCount = !blockTx.IsCoinbase ? blockTx.Transaction.Inputs.Length : 0;
                    var completionCount = new CompletionCount(inputCount + 1);
                    pendingWarmedTxes.Enqueue(Tuple.Create(blockTx, completionCount));

                    var txHashes = new Tuple<UInt256, CompletionCount>[inputCount + 1];

                    txHashes[0] = Tuple.Create(blockTx.Hash, completionCount);

                    if (!blockTx.IsCoinbase)
                    {
                        for (var inputIndex = 0; inputIndex < blockTx.Transaction.Inputs.Length; inputIndex++)
                        {
                            var txHash = blockTx.Transaction.Inputs[inputIndex].PreviousTxOutputKey.TxHash;
                            txHashes[inputIndex + 1] = Tuple.Create(txHash, completionCount);
                        }
                    }

                    return txHashes;
                },
                new ExecutionDataflowBlockOptions { CancellationToken = cancelToken });
        }

        private static TransformManyBlock<Tuple<UInt256, CompletionCount>, object> InitWarmupUtxo(IDeferredChainStateCursor deferredChainStateCursor, CancellationToken cancelToken)
        {
            return new TransformManyBlock<Tuple<UInt256, CompletionCount>, object>(
                tuple =>
                {
                    var txHash = tuple.Item1;
                    var completionCount = tuple.Item2;

                    deferredChainStateCursor.WarmUnspentTx(txHash);

                    // return a 1 element to trigger ForwardWarmedTxes
                    if (completionCount.TryComplete())
                        return new object[1];
                    else
                        return new object[0];
                },
                new ExecutionDataflowBlockOptions { CancellationToken = cancelToken, MaxDegreeOfParallelism = 16 });
        }

        private static TransformManyBlock<object, BlockTx> InitForwardWarmedTxes(ConcurrentQueue<Tuple<BlockTx, CompletionCount>> pendingWarmedTxes, CancellationToken cancelToken)
        {
            return new TransformManyBlock<object, BlockTx>(
                _ =>
                {
                    // return any fully warmed txes, preserving the tx order
                    var warmedTxes = new List<BlockTx>();

                    Tuple<BlockTx, CompletionCount> tuple;
                    while (pendingWarmedTxes.TryPeek(out tuple) && tuple.Item2.IsComplete)
                    {
                        warmedTxes.Add(tuple.Item1);
                        pendingWarmedTxes.TryDequeue(out tuple);
                    }

                    return warmedTxes;
                },
                new ExecutionDataflowBlockOptions { CancellationToken = cancelToken });
        }
    }
}
