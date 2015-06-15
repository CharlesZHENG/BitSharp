using BitSharp.Common;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;

namespace BitSharp.Core.Builders
{
    internal class TxLoader : IDisposable
    {
        private readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly ChainStateBuilder.BuilderStats stats;
        private readonly CoreStorage coreStorage;

        private readonly ParallelConsumer<LoadingTx> inputTxQueuer;
        private readonly ParallelConsumer<Tuple<LoadingTx, int>> inputTxLoader;

        public TxLoader(string name, ChainStateBuilder.BuilderStats stats, CoreStorage coreStorage, int threadCount)
        {
            this.stats = stats;
            this.coreStorage = coreStorage;

            this.inputTxQueuer = new ParallelConsumer<LoadingTx>(name + ".PrevTxLoader.1", 1);
            this.inputTxLoader = new ParallelConsumer<Tuple<LoadingTx, int>>(name + ".PrevTxLoader.2", threadCount);
        }

        public void Dispose()
        {
            this.inputTxQueuer.Dispose();
            this.inputTxLoader.Dispose();
        }

        public int PendingCount { get { return this.inputTxLoader.PendingCount; } }

        public void LoadTxes(ConcurrentBlockingQueue<LoadingTx> loadingTxes, Action<ConcurrentBlockingQueue<LoadedTx>> workAction)
        {
            using (var loadingTxInputs = new ConcurrentBlockingQueue<Tuple<LoadingTx, int>>())
            using (var loadedTxes = new ConcurrentBlockingQueue<LoadedTx>())
            using (StartInputTxQueuer(loadingTxes, loadingTxInputs, loadedTxes))
            using (StartInputTxLoader(loadingTxInputs, loadedTxes))
            {
                workAction(loadedTxes);
            }
        }

        private IDisposable StartInputTxQueuer(ConcurrentBlockingQueue<LoadingTx> loadingTxes, ConcurrentBlockingQueue<Tuple<LoadingTx, int>> loadingTxInputs, ConcurrentBlockingQueue<LoadedTx> loadedTxes)
        {
            return this.inputTxQueuer.Start(loadingTxes,
                loadingTx =>
                {
                    // queue up inputs to be loaded for non-coinbase transactions
                    if (loadingTx.TxIndex > 0)
                    {
                        for (var inputIndex = 0; inputIndex < loadingTx.PrevOutputTxKeys.Length; inputIndex++)
                            loadingTxInputs.Add(Tuple.Create(loadingTx, inputIndex));
                    }
                    // no inputs to load for the coinbase transactions, queue loaded tx immediately
                    else
                    {
                        loadedTxes.Add(new LoadedTx(loadingTx.Transaction, loadingTx.TxIndex, ImmutableArray.Create<Transaction>()));
                    }
                },
                _ => loadingTxInputs.CompleteAdding());
        }

        private IDisposable StartInputTxLoader(ConcurrentBlockingQueue<Tuple<LoadingTx, int>> loadingTxInputs, ConcurrentBlockingQueue<LoadedTx> loadedTxes)
        {
            return this.inputTxLoader.Start(loadingTxInputs,
                loadingTxInput =>
                {
                    var loadingTx = loadingTxInput.Item1;
                    var inputIndex = loadingTxInput.Item2;

                    var loadedTx = LoadTxInput(loadingTx, inputIndex);
                    if (loadedTx != null)
                        loadedTxes.Add(loadedTx);
                },
                _ =>
                {
                    loadedTxes.CompleteAdding();
                });
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
                if (this.stats != null)
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
