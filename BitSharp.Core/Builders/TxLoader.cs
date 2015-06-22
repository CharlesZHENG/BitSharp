using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Disposables;
using System.Reactive;
using System.Threading;
using System.Collections.Generic;

namespace BitSharp.Core.Builders
{
    internal class TxLoader : IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly ChainStateBuilder.BuilderStats stats;
        private readonly CoreStorage coreStorage;

        private readonly ParallelReader<Tuple<LoadingTx, int>> loadTxInputsSource;
        private readonly ParallelObserver<Tuple<LoadingTx, int>> inputTxLoader;

        public TxLoader(string name, ChainStateBuilder.BuilderStats stats, CoreStorage coreStorage, int threadCount)
        {
            this.stats = stats;
            this.coreStorage = coreStorage;

            this.loadTxInputsSource = new ParallelReader<Tuple<LoadingTx, int>>(name + ".LoadTxInputsSource");
            this.inputTxLoader = new ParallelObserver<Tuple<LoadingTx, int>>(name + ".InputTxLoader", threadCount);
        }

        public void Dispose()
        {
            this.loadTxInputsSource.Dispose();
            this.inputTxLoader.Dispose();
        }

        public int PendingCount { get { return this.inputTxLoader.PendingCount; } }

        public IEnumerable<LoadedTx> LoadTxes(ParallelReader<LoadingTx> loadingTxes)
        {
            using (var loadedTxes = new ConcurrentBlockingQueue<LoadedTx>())
            using (var loadTxInputsTask = this.loadTxInputsSource.ReadAsync(StartInputTxQueuer(loadingTxes, loadedTxes)).WaitOnDispose())
            using (var inputTxLoaderTask = this.inputTxLoader.SubscribeObservers(loadTxInputsSource, StartInputTxLoader(loadedTxes)).WaitOnDispose())
            {
                foreach (var loadedTx in loadedTxes.GetConsumingEnumerable())
                    yield return loadedTx;
            }
        }

        private IEnumerable<Tuple<LoadingTx, int>> StartInputTxQueuer(ParallelReader<LoadingTx> loadingTxes, ConcurrentBlockingQueue<LoadedTx> loadedTxes)
        {
            foreach (var loadingTx in loadingTxes.GetConsumingEnumerable())
            {
                // queue up inputs to be loaded for non-coinbase transactions
                if (!loadingTx.IsCoinbase)
                {
                    for (var inputIndex = 0; inputIndex < loadingTx.PrevOutputTxKeys.Length; inputIndex++)
                        yield return Tuple.Create(loadingTx, inputIndex);
                }
                // no inputs to load for the coinbase transactions, queue loaded tx immediately
                else
                {
                    loadedTxes.Add(new LoadedTx(loadingTx.Transaction, loadingTx.TxIndex, ImmutableArray.Create<Transaction>()));
                }
            }
        }

        private IObserver<Tuple<LoadingTx, int>> StartInputTxLoader(ConcurrentBlockingQueue<LoadedTx> loadedTxes)
        {
            return Observer.Create<Tuple<LoadingTx, int>>(
                tuple =>
                {
                    var loadingTx = tuple.Item1;
                    var inputIndex = tuple.Item2;

                    var loadedTx = LoadTxInput(loadingTx, inputIndex);
                    if (loadedTx != null)
                        loadedTxes.Add(loadedTx);
                },
                ex => loadedTxes.CompleteAdding(),
                () => loadedTxes.CompleteAdding());
        }

        private LoadedTx LoadTxInput(LoadingTx loadingTx, int inputIndex)
        {
            var txIndex = loadingTx.TxIndex;
            var transaction = loadingTx.Transaction;
            var chainedHeader = loadingTx.ChainedHeader;
            var prevOutputTxKey = loadingTx.PrevOutputTxKeys[inputIndex];

            // load previous transactions for each input, unless this is a coinbase transaction
            var input = transaction.Inputs[inputIndex];
            var inputPrevTxHash = input.PreviousTxOutputKey.TxHash;

            Transaction inputPrevTx;
            var stopwatch = Stopwatch.StartNew();
            if (coreStorage.TryGetTransaction(prevOutputTxKey.BlockHash, prevOutputTxKey.TxIndex, out inputPrevTx))
            {
                stopwatch.Stop();
                if (this.stats != null && chainedHeader.Height > 0)
                {
                    this.stats.prevTxLoadDurationMeasure.Tick(stopwatch.Elapsed);
                    this.stats.prevTxLoadRateMeasure.Tick();
                }

                if (input.PreviousTxOutputKey.TxHash != inputPrevTx.Hash)
                    throw new Exception("TODO");

            }
            else
                throw new Exception("TODO");

            if (loadingTx.InputTxes.TryComplete(inputIndex, inputPrevTx))
                return loadingTx.ToLoadedTx();
            else
                return null;
        }
    }
}
