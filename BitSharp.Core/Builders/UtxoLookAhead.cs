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
        private readonly ParallelObservable<Tuple<UInt256, int, CompletionCount>> blockTxesDispatcher;
        private readonly ParallelObserver<Tuple<UInt256, int, CompletionCount>> utxoReader;
        private readonly ParallelObservable<BlockTx> blockTxesSorter;

        private readonly ChainStateBuilder.BuilderStats stats;

        public UtxoLookAhead(ChainStateBuilder.BuilderStats stats)
        {
            this.blockTxesDispatcher = new ParallelObservable<Tuple<UInt256, int, CompletionCount>>("UtxoLookAhead.BlockTxesDispatcher");
            this.utxoReader = new ParallelObserver<Tuple<UInt256, int, CompletionCount>>("UtxoLookAhead.UtxoReader", 4);
            this.blockTxesSorter = new ParallelObservable<BlockTx>("UtxoLookAhead.BlockTxesSorter");
            this.stats = stats;
        }

        public void Dispose()
        {
            this.blockTxesDispatcher.Dispose();
            this.utxoReader.Dispose();
            this.blockTxesSorter.Dispose();
        }

        public IEnumerable<BlockTx> LookAhead(IEnumerable<BlockTx> blockTxes, IChainState chainState, DeferredChainStateCursor deferredChainStateCursor)
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
                                    .Select((txInput, inputIndex) => Tuple.Create(txInput.PreviousTxOutputKey.TxHash, inputIndex, completionCount))
                                .Concat(Tuple.Create(blockTx.Hash, inputCount, completionCount));
                        })
                    .ToObservable()
                    .Finally(() =>
                    {
                        completedAdding = true;
                        txLoadedEvent.Set();
                    }));

                // create the observable source of warmed-up transactions, in the original block order
                var warmedTxesSource = this.blockTxesSorter.Create(
                    Observable.Create(
                        (Func<IObserver<BlockTx>, IDisposable>)(
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
                        }))
                    );

                using (this.utxoReader.SubscribeObservers(utxoWarmupSource,
                    Observer.Create<Tuple<UInt256, int, CompletionCount>>(
                        onNext: tuple =>
                        {
                            var txHash = tuple.Item1;
                            var inputIndex = tuple.Item2;
                            var completionCount = tuple.Item3;

                            UnspentTx unspentTx;
                            deferredChainStateCursor.WarmUnspentTx(txHash, () =>
                            {
                                var result = Tuple.Create(chainState.TryGetUnspentTx(txHash, out unspentTx), unspentTx);
                                this.stats.utxoReadRateMeasure.Tick();
                                return result;
                            });

                            if (completionCount.TryComplete())
                                txLoadedEvent.Set();
                        })))
                using (var warmedTxesQueue = new ConcurrentBlockingQueue<BlockTx>())
                using (warmedTxesSource.Subscribe(
                    blockTx => warmedTxesQueue.Add(blockTx),
                    ex => warmedTxesQueue.CompleteAdding(),
                    () => warmedTxesQueue.CompleteAdding()))
                {
                    foreach (var blockTx in warmedTxesQueue.GetConsumingEnumerable())
                        yield return blockTx;
                }
            }
        }
    }
}
