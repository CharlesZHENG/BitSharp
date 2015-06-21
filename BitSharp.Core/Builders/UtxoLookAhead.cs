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
            using (var progress = new Progress())
            {
                // create the observable source of each utxo entry to be warmed up: each input's previous transaction, and each new transaction
                var utxoWarmupSource = this.blockTxesDispatcher.Create(CreateBlockTxesSource(progress, blockTxes));

                // subscribe the utxo entries to be warmed up
                using (this.utxoReader.SubscribeObservers(utxoWarmupSource, CreateUtxoWarmer(progress, deferredChainStateCursor)))
                {
                    // create the observable source of warmed-up transactions, in the original block order
                    var warmedTxesSource = this.blockTxesSorter.Create(CreateWarmedTxesSource(progress));

                    // return the warmed-up transactions, in the original block order
                    foreach (var blockTx in warmedTxesSource.ToEnumerable())
                        yield return blockTx;
                }
            }
        }

        private IObservable<Tuple<UInt256, CompletionCount>> CreateBlockTxesSource(Progress progress, IEnumerable<BlockTx> blockTxes)
        {
            return
                blockTxes.SelectMany(
                    blockTx =>
                    {
                        var inputCount = !blockTx.IsCoinbase ? blockTx.Transaction.Inputs.Length : 0;
                        var completionCount = new CompletionCount(inputCount + 1);
                        progress.pendingWarmedTxes.Enqueue(Tuple.Create(blockTx, completionCount));

                        return
                            blockTx.Transaction.Inputs
                                .TakeWhile(_ => !blockTx.IsCoinbase)
                                .Select(txInput => Tuple.Create(txInput.PreviousTxOutputKey.TxHash, completionCount))
                            .Concat(Tuple.Create(blockTx.Hash, completionCount));
                    })
                .ToObservable()
                .Finally(() =>
                {
                    progress.completedAdding = true;
                    progress.txLoadedEvent.Set();
                });
        }

        private IObserver<Tuple<UInt256, CompletionCount>> CreateUtxoWarmer(Progress progress, DeferredChainStateCursor deferredChainStateCursor)
        {
            return Observer.Create<Tuple<UInt256, CompletionCount>>(
                tuple =>
                {
                    var txHash = tuple.Item1;
                    var completionCount = tuple.Item2;

                    deferredChainStateCursor.WarmUnspentTx(txHash);

                    if (completionCount.TryComplete())
                        progress.txLoadedEvent.Set();
                });
        }

        private IObservable<BlockTx> CreateWarmedTxesSource(Progress progress)
        {
            return Observable.Create<BlockTx>(
                observer =>
                {
                    Tuple<BlockTx, CompletionCount> tuple;
                    while (!progress.completedAdding || progress.pendingWarmedTxes.Count > 0)
                    {
                        progress.txLoadedEvent.WaitOne();

                        while (progress.pendingWarmedTxes.TryPeek(out tuple) && tuple.Item2.IsComplete)
                        {
                            observer.OnNext(tuple.Item1);
                            progress.pendingWarmedTxes.TryDequeue(out tuple);
                        }
                    }

                    observer.OnCompleted();

                    return Disposable.Empty;
                });
        }

        //TODO
        private sealed class Progress : IDisposable
        {
            public AutoResetEvent txLoadedEvent = new AutoResetEvent(false);
            public ConcurrentQueue<Tuple<BlockTx, CompletionCount>> pendingWarmedTxes = new ConcurrentQueue<Tuple<BlockTx, CompletionCount>>();
            public bool completedAdding = false;

            public void Dispose()
            {
                txLoadedEvent.Dispose();
            }
        }
    }
}
