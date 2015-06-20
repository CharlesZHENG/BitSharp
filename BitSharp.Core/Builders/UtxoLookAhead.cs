using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Threading;

namespace BitSharp.Core.Builders
{
    internal class UtxoLookAhead : IDisposable
    {
        private readonly ParallelObservable<Tuple<UInt256, CompletionCount>> blockTxesDispatcher;
        private readonly ParallelObserver<Tuple<UInt256, CompletionCount>> utxoReader;
        private readonly ParallelObservable<BlockTx> blockTxesSorter;

        public UtxoLookAhead()
        {
            this.blockTxesDispatcher = new ParallelObservable<Tuple<UInt256, CompletionCount>>("UtxoLookAhead.BlockTxesDispatcher");
            this.utxoReader = new ParallelObserver<Tuple<UInt256, CompletionCount>>("UtxoLookAhead.UtxoReader", 4);
            this.blockTxesSorter = new ParallelObservable<BlockTx>("UtxoLookAhead.BlockTxesSorter");
        }

        public void Dispose()
        {
            this.blockTxesDispatcher.Dispose();
            this.utxoReader.Dispose();
            this.blockTxesSorter.Dispose();
        }

        public IEnumerable<BlockTx> LookAhead(IEnumerable<BlockTx> blockTxes, DeferredChainStateCursor deferredChainStateCursor)
        {
            var pendingWarmedTxes = new ConcurrentQueue<Tuple<BlockTx, CompletionCount>>();
            var completedAdding = false;

            using (var txLoadedEvent = new AutoResetEvent(false))
            {
                // create the observable source of each utxo entry to be warmed up: each input's previous transactions, and each new transaction
                var utxoWarmupSource = this.blockTxesDispatcher.Create(
                    blockTxes.SelectMany(
                        blockTx =>
                        {
                            var inputCount = !blockTx.IsCoinbase ? blockTx.Transaction.Inputs.Length : 0;
                            var completionCount = new CompletionCount(inputCount + 1);
                            pendingWarmedTxes.Enqueue(Tuple.Create(blockTx, completionCount));

                            return
                                blockTx.Transaction.Inputs
                                    .TakeWhile(_ => !blockTx.IsCoinbase)
                                    .Select(txInput => Tuple.Create(txInput.PreviousTxOutputKey.TxHash, completionCount))
                                .Concat(Tuple.Create(blockTx.Hash, completionCount));
                        })
                    .ToObservable()
                    .Finally(() =>
                    {
                        completedAdding = true;
                        txLoadedEvent.Set();
                    }));

                // create the observable source of warmed-up transactions, in the original block order
                var warmedTxesSource = this.blockTxesSorter.Create(
                    Observable.Create<BlockTx>(
                        observer =>
                        {
                            Tuple<BlockTx, CompletionCount> tuple;
                            while (!completedAdding || pendingWarmedTxes.Count > 0)
                            {
                                txLoadedEvent.WaitOne();

                                while (pendingWarmedTxes.TryPeek(out tuple) && tuple.Item2.IsComplete)
                                {
                                    observer.OnNext(tuple.Item1);
                                    pendingWarmedTxes.TryDequeue(out tuple);
                                }
                            }

                            observer.OnCompleted();
                            return Disposable.Empty;
                        })
                    );

                using (this.utxoReader.SubscribeObservers(utxoWarmupSource,
                    Observer.Create<Tuple<UInt256, CompletionCount>>(
                        onNext: tuple =>
                        {
                            var txHash = tuple.Item1;
                            var completionCount = tuple.Item2;

                            deferredChainStateCursor.WarmUnspentTx(txHash);

                            if (completionCount.TryComplete())
                                txLoadedEvent.Set();
                        })))
                using (var warmedTxesQueue = new ConcurrentBlockingQueue<BlockTx>())
                using (warmedTxesSource.Subscribe(
                    blockTx => warmedTxesQueue.Add(blockTx),
                    ex => warmedTxesQueue.CompleteAdding(),
                    () => warmedTxesQueue.CompleteAdding()))
                {
                    // yield the warmed-up transactions, in the original block order
                    foreach (var blockTx in warmedTxesQueue.GetConsumingEnumerable())
                        yield return blockTx;
                }
            }
        }
    }
}
