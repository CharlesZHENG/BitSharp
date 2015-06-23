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
using System.Threading.Tasks;

namespace BitSharp.Core.Builders
{
    internal class TxLoader : IParallelReader<LoadedTx>, IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly ReaderWriterLockSlim controlLock = new ReaderWriterLockSlim();
        private readonly ICoreStorage coreStorage;

        private readonly ParallelConsumerProducer<LoadingTx, Tuple<LoadingTx, int>> loadTxInputsSource;
        private readonly ParallelConsumerProducer<Tuple<LoadingTx, int>, LoadedTx> inputTxLoader;

        private TaskCompletionSource<object> tcs;
        private ParallelReader<LoadingTx> loadingTxesReader;

        public TxLoader(string name, ICoreStorage coreStorage, int threadCount)
        {
            this.coreStorage = coreStorage;

            loadTxInputsSource = new ParallelConsumerProducer<LoadingTx, Tuple<LoadingTx, int>>(name + ".LoadTxInputsSource", threadCount);
            inputTxLoader = new ParallelConsumerProducer<Tuple<LoadingTx, int>, LoadedTx>(name + ".InputTxLoader", threadCount);
        }

        public void Dispose()
        {
            loadTxInputsSource.Dispose();
            inputTxLoader.Dispose();
            controlLock.Dispose();
        }

        public bool IsStarted
        {
            get { return inputTxLoader.IsStarted; }
        }

        public int Count
        {
            get { return loadingTxesReader.Count + inputTxLoader.Count; }
        }

        public Task LoadTxes(ParallelReader<LoadingTx> loadingTxesReader)
        {
            controlLock.EnterWriteLock();
            try
            {
                if (tcs != null)
                    throw new InvalidOperationException();

                tcs = new TaskCompletionSource<object>();
                this.loadingTxesReader = loadingTxesReader;

                loadTxInputsSource.ConsumeProduceAsync(loadingTxesReader, null,
                    loadingTx => QueueLoadingTxInputs(loadingTx));

                inputTxLoader.ConsumeProduceAsync(loadTxInputsSource, null,
                    tuple => LoadTxInputAndQueueLoadedTx(tuple),
                    finallyAction: ex => Finish(ex));

                return tcs.Task;
            }
            finally
            {
                controlLock.ExitWriteLock();
            }
        }

        public IEnumerable<LoadedTx> GetConsumingEnumerable()
        {
            return inputTxLoader.GetConsumingEnumerable();
        }

        public void Wait()
        {
            loadTxInputsSource.Wait();
            inputTxLoader.Wait();
        }

        public void Cancel(Exception ex)
        {
            loadTxInputsSource.Cancel(ex);
            inputTxLoader.Cancel(ex);
        }

        private IEnumerable<Tuple<LoadingTx, int>> QueueLoadingTxInputs(LoadingTx loadingTx)
        {
            // queue up inputs to be loaded for non-coinbase transactions
            if (!loadingTx.IsCoinbase)
            {
                for (var inputIndex = 0; inputIndex < loadingTx.PrevOutputTxKeys.Length; inputIndex++)
                    yield return Tuple.Create(loadingTx, inputIndex);
            }
            // queue coinbase transaction with no inputs to load
            else
            {
                yield return Tuple.Create(loadingTx, -1);
            }
        }

        private IEnumerable<LoadedTx> LoadTxInputAndQueueLoadedTx(Tuple<LoadingTx, int> tuple)
        {
            var loadingTx = tuple.Item1;
            var inputIndex = tuple.Item2;

            var loadedTx = LoadTxInput(loadingTx, inputIndex);
            if (loadedTx != null)
                yield return loadedTx;
        }

        private LoadedTx LoadTxInput(LoadingTx loadingTx, int inputIndex)
        {
            var txIndex = loadingTx.TxIndex;
            var transaction = loadingTx.Transaction;
            var chainedHeader = loadingTx.ChainedHeader;

            if (!loadingTx.IsCoinbase)
            {
                var prevOutputTxKey = loadingTx.PrevOutputTxKeys[inputIndex];

                // load previous transactions for each input, unless this is a coinbase transaction
                var input = transaction.Inputs[inputIndex];
                var inputPrevTxHash = input.PreviousTxOutputKey.TxHash;

                Transaction inputPrevTx;
                if (coreStorage.TryGetTransaction(prevOutputTxKey.BlockHash, prevOutputTxKey.TxIndex, out inputPrevTx))
                {
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
            else
            {
                Debug.Assert(inputIndex == -1);
                return new LoadedTx(transaction, txIndex, ImmutableArray.Create<Transaction>());
            }
        }

        private void Finish(Exception ex = null)
        {
            controlLock.EnterUpgradeableReadLock();
            try
            {
                try
                {
                    loadTxInputsSource.Wait();
                    inputTxLoader.Wait();
                }
                catch (Exception taskEx)
                {
                    ex = ex ?? taskEx;
                }

                controlLock.EnterWriteLock();
                try
                {
                    if (ex != null)
                        tcs.SetException(ex);
                    else
                        tcs.SetResult(null);

                    tcs = null;
                }
                finally
                {
                    controlLock.ExitWriteLock();
                }
            }
            finally
            {
                controlLock.ExitUpgradeableReadLock();
            }
        }
    }
}
