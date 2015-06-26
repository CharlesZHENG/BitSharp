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
    internal class UtxoLookAhead : IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private static readonly DurationMeasure txesReadDurationMeasure = new DurationMeasure(TimeSpan.FromMinutes(5), TimeSpan.FromSeconds(5));
        private static readonly DurationMeasure lookAheadDurationMeasure = new DurationMeasure(TimeSpan.FromMinutes(5), TimeSpan.FromSeconds(5));

        private readonly IEnumerable<BlockTx> blockTxes;
        private readonly DeferredChainStateCursor deferredChainStateCursor;

        private readonly CancellationTokenSource cancelToken = new CancellationTokenSource();
        private readonly Task completion;

        private readonly AutoResetEvent txLoadedEvent = new AutoResetEvent(false);
        private readonly ConcurrentQueue<Tuple<BlockTx, CompletionCount>> pendingWarmedTxes = new ConcurrentQueue<Tuple<BlockTx, CompletionCount>>();
        private TransformManyBlock<BlockTx, Tuple<UInt256, CompletionCount>> queueUnspentTxLookup;
        private BlockingCollection<BlockTx> warmedTxes;
        private bool completeAdding;

        public UtxoLookAhead(IEnumerable<BlockTx> blockTxes, DeferredChainStateCursor deferredChainStateCursor)
        {
            this.blockTxes = blockTxes;
            this.deferredChainStateCursor = deferredChainStateCursor;

            completion = Task.Factory.StartNew(async () => await LookAhead(), cancelToken.Token, TaskCreationOptions.AttachedToParent, TaskScheduler.Default);
        }

        public void Dispose()
        {
            txLoadedEvent.Dispose();
            cancelToken.Dispose();
        }

        public Task Completion { get { return completion; } }

        public IEnumerable<BlockTx> ConsumeWarmedTxes()
        {
            using (var enumerator = warmedTxes.GetConsumingEnumerable(cancelToken.Token).GetEnumerator())
            {
                bool result;
                do
                {
                    try
                    {
                        result = enumerator.MoveNext();
                    }
                    //TODO
                    catch (OperationCanceledException)
                    {
                        result = false;
                    }

                    if (result)
                        yield return enumerator.Current;
                }
                while (result);
            }
        }

        private async Task LookAhead()
        {
            try
            {
                var stopwatch = Stopwatch.StartNew();
                warmedTxes = new BlockingCollection<BlockTx>();

                // queue each utxo entry to be warmed up: each input's previous transaction, and each new transaction
                queueUnspentTxLookup = InitQueueUnspentTxLookup();

                // warm up a uxto entry
                var warmupUtxo = InitWarmupUtxo();

                // link the utxo entry queuer to the warmer
                queueUnspentTxLookup.LinkTo(warmupUtxo, new DataflowLinkOptions { PropagateCompletion = true });

                // begin feeding the unspent tx queuer
                var readBlockTxes = Task.Factory.StartNew(() =>
                {
                    foreach (var blockTx in blockTxes)
                        queueUnspentTxLookup.Post(blockTx);

                    queueUnspentTxLookup.Complete();

                }, cancelToken.Token, TaskCreationOptions.AttachedToParent, TaskScheduler.Default);

                // begin posting warmed txes
                var postWarmedTxes = Task.Factory.StartNew(PostWarmedTxes, cancelToken.Token, TaskCreationOptions.AttachedToParent, TaskScheduler.Default);

                await readBlockTxes;

                txesReadDurationMeasure.Tick(stopwatch.Elapsed);
                Throttler.IfElapsed(TimeSpan.FromSeconds(5), () =>
                    logger.Info("Block Txes Read: {0,12:N3}ms".Format2(txesReadDurationMeasure.GetAverage().TotalMilliseconds)));

                await queueUnspentTxLookup.Completion;

                completeAdding = true;
                txLoadedEvent.Set();

                await warmupUtxo.Completion;
                await postWarmedTxes;

                lookAheadDurationMeasure.Tick(stopwatch.Elapsed);
                Throttler.IfElapsed(TimeSpan.FromSeconds(5), () =>
                    logger.Info("UTXO Look-ahead: {0,12:N3}ms".Format2(lookAheadDurationMeasure.GetAverage().TotalMilliseconds)));
            }
            finally
            {
                // ensure any consumers of the warmed txes queue are unblocked if warming failed to complete
                if (!warmedTxes.IsAddingCompleted)
                {
                    cancelToken.Cancel();
                    txLoadedEvent.Set();
                }
            }
        }

        private TransformManyBlock<BlockTx, Tuple<UInt256, CompletionCount>> InitQueueUnspentTxLookup()
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
                new ExecutionDataflowBlockOptions { CancellationToken = cancelToken.Token });
        }

        private ActionBlock<Tuple<UInt256, CompletionCount>> InitWarmupUtxo()
        {
            return new ActionBlock<Tuple<UInt256, CompletionCount>>(
                tuple =>
                {
                    var txHash = tuple.Item1;
                    var completionCount = tuple.Item2;

                    deferredChainStateCursor.WarmUnspentTx(txHash);

                    if (completionCount.TryComplete())
                        txLoadedEvent.Set();
                },
                new ExecutionDataflowBlockOptions { CancellationToken = cancelToken.Token, MaxDegreeOfParallelism = 16 });
        }

        private void PostWarmedTxes()
        {
            try
            {
                Tuple<BlockTx, CompletionCount> tuple;
                while (!completeAdding || !pendingWarmedTxes.IsEmpty)
                {
                    txLoadedEvent.WaitOne();
                    cancelToken.Token.ThrowIfCancellationRequested();

                    while (pendingWarmedTxes.TryPeek(out tuple) && tuple.Item2.IsComplete)
                    {
                        warmedTxes.Add(tuple.Item1);
                        pendingWarmedTxes.TryDequeue(out tuple);
                    }
                }

                warmedTxes.CompleteAdding();
            }
            catch (Exception)
            {
                throw;
            }
        }
    }
}
