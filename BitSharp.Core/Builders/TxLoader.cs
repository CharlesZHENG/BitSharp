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
using System.Threading.Tasks.Dataflow;

namespace BitSharp.Core.Builders
{
    internal class TxLoader : IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly ICoreStorage coreStorage;
        private readonly int threadCount;
        private readonly ParallelReader<LoadingTx> loadingTxesReader;

        private readonly CancellationTokenSource cancelToken = new CancellationTokenSource();
        private readonly Task completion;

        private TransformManyBlock<LoadingTx, Tuple<LoadingTx, int>> createTxInputList;
        private TransformManyBlock<Tuple<LoadingTx, int>, LoadedTx> loadTxInputAndReturnLoadedTx;

        public TxLoader(string name, ICoreStorage coreStorage, int threadCount, ParallelReader<LoadingTx> loadingTxesReader)
        {
            this.coreStorage = coreStorage;
            this.threadCount = threadCount;
            this.loadingTxesReader = loadingTxesReader;

            completion = LoadTxes();
        }

        public void Dispose()
        {
            cancelToken.Dispose();
        }

        public Task Completion { get { return completion; } }

        //TODO remove
        public bool IsStarted
        {
            get { return true; }
        }

        //TODO remove
        public int Count
        {
            get { return 0; }
        }

        public ISourceBlock<LoadedTx> LoadedTxes
        {
            get
            {
                return loadTxInputAndReturnLoadedTx;
            }
        }

        private async Task LoadTxes()
        {
            // split incoming LoadingTx by its number of inputs
            createTxInputList = InitCreateTxInputList();

            // load each input, and return and fully loaded txes
            loadTxInputAndReturnLoadedTx = InitLoadTxInputAndReturnLoadedTx();

            // link the input splitter to the input loader
            createTxInputList.LinkTo(loadTxInputAndReturnLoadedTx, new DataflowLinkOptions { PropagateCompletion = true });

            // begin feeding the input splitter
            var readLoadingTxes = Task.Factory.StartNew(() =>
            {
                foreach (var loadingTx in loadingTxesReader.GetConsumingEnumerable())
                    createTxInputList.Post(loadingTx);

                createTxInputList.Complete();
            }, cancelToken.Token, TaskCreationOptions.AttachedToParent, TaskScheduler.Default);

            await readLoadingTxes;
            await createTxInputList.Completion;
            await loadTxInputAndReturnLoadedTx.Completion;
        }

        public void Wait()
        {
            completion.Wait();
        }

        public void Cancel(Exception ex)
        {
            cancelToken.Cancel();
        }

        private TransformManyBlock<LoadingTx, Tuple<LoadingTx, int>> InitCreateTxInputList()
        {
            return new TransformManyBlock<LoadingTx, Tuple<LoadingTx, int>>(
                loadingTx =>
                {
                    // queue up inputs to be loaded for non-coinbase transactions
                    if (!loadingTx.IsCoinbase)
                    {
                        return Enumerable.Range(0, loadingTx.PrevOutputTxKeys.Length).Select(
                            inputIndex => Tuple.Create(loadingTx, inputIndex));
                    }
                    // queue coinbase transaction with no inputs to load
                    else
                    {
                        return new[] { Tuple.Create(loadingTx, -1) };
                    }
                },
                new ExecutionDataflowBlockOptions { CancellationToken = cancelToken.Token });
        }

        private TransformManyBlock<Tuple<LoadingTx, int>, LoadedTx> InitLoadTxInputAndReturnLoadedTx()
        {
            return new TransformManyBlock<Tuple<LoadingTx, int>, LoadedTx>(
                tuple =>
                {
                    var loadingTx = tuple.Item1;
                    var inputIndex = tuple.Item2;

                    var loadedTx = LoadTxInput(loadingTx, inputIndex);
                    if (loadedTx != null)
                        return new[] { loadedTx };
                    else
                        return new LoadedTx[0];
                },
                new ExecutionDataflowBlockOptions { CancellationToken = cancelToken.Token, MaxDegreeOfParallelism = 16 });
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
    }
}
