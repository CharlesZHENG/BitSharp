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
        private readonly ParallelReader<Tuple<UInt256, CompletionCount>> utxoWarmupSource;
        private readonly ParallelObserver<Tuple<UInt256, CompletionCount>> utxoReader;

        public UtxoLookAhead()
        {
            this.utxoWarmupSource = new ParallelReader<Tuple<UInt256, CompletionCount>>("UtxoLookAhead.UtxoWarmupSource");
            this.utxoReader = new ParallelObserver<Tuple<UInt256, CompletionCount>>("UtxoLookAhead.UtxoReader", 4);
        }

        public void Dispose()
        {
            this.utxoWarmupSource.Dispose();
            this.utxoReader.Dispose();
        }

        public IEnumerable<BlockTx> LookAhead(IEnumerable<BlockTx> blockTxes, DeferredChainStateCursor deferredChainStateCursor)
        {
            using (var progress = new Progress())
            // create the observable source of each utxo entry to be warmed up: each input's previous transaction, and each new transaction
            using (var utxoWarmupTask = this.utxoWarmupSource.ReadAsync(CreateBlockTxesSource(progress, blockTxes)).WaitOnDispose())
            // subscribe the utxo entries to be warmed up
            using (var utxoReaderTask = this.utxoReader.SubscribeObservers(utxoWarmupSource, CreateUtxoWarmer(progress, deferredChainStateCursor)).WaitOnDispose())
            {

                // return the warmed-up transactions, in the original block order
                foreach (var blockTx in CreateWarmedTxesSource(progress))
                    yield return blockTx;
            }
        }

        private IEnumerable<Tuple<UInt256, CompletionCount>> CreateBlockTxesSource(Progress progress, IEnumerable<BlockTx> blockTxes)
        {
            try
            {
                foreach (var blockTx in blockTxes)
                {
                    var inputCount = !blockTx.IsCoinbase ? blockTx.Transaction.Inputs.Length : 0;
                    var completionCount = new CompletionCount(inputCount + 1);
                    progress.pendingWarmedTxes.Enqueue(Tuple.Create(blockTx, completionCount));

                    yield return Tuple.Create(blockTx.Hash, completionCount);

                    if (!blockTx.IsCoinbase)
                        foreach (var txHash in blockTx.Transaction.Inputs.Select(x => x.PreviousTxOutputKey.TxHash))
                            yield return Tuple.Create(txHash, completionCount);
                }
            }
            finally
            {
                progress.completedAdding = true;
                progress.txLoadedEvent.Set();
            }
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

        private IEnumerable<BlockTx> CreateWarmedTxesSource(Progress progress)
        {
            Tuple<BlockTx, CompletionCount> tuple;
            while (!progress.completedAdding || progress.pendingWarmedTxes.Count > 0)
            {
                progress.txLoadedEvent.WaitOne();

                while (progress.pendingWarmedTxes.TryPeek(out tuple) && tuple.Item2.IsComplete)
                {
                    yield return tuple.Item1;
                    progress.pendingWarmedTxes.TryDequeue(out tuple);
                }
            }
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
