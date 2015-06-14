using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;

namespace BitSharp.Core.Builders
{
    internal class PrevTxLoader : IDisposable
    {
        private readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly ChainStateBuilder.BuilderStats stats;
        private readonly CoreStorage coreStorage;

        private readonly ParallelConsumer<TxWithInputTxLookupKeys> txDispatcher;
        private readonly ParallelConsumer<TxInputWithPrevOutputKey> txLoader;

        private ConcurrentDictionary<UInt256, Transaction[]> loadingTxes;
        private ConcurrentBlockingQueue<TxInputWithPrevOutputKey> pendingTxes;
        private ConcurrentBlockingQueue<LoadedTx> loadedTxes;
        private IDisposable txDispatcherStopper;
        private IDisposable txLoaderStopper;

        public PrevTxLoader(string name, ChainStateBuilder.BuilderStats stats, CoreStorage coreStorage, int threadCount)
        {
            this.stats = stats;
            this.coreStorage = coreStorage;

            this.txDispatcher = new ParallelConsumer<TxWithInputTxLookupKeys>(name + ".PrevTxLoader.1", 1);
            this.txLoader = new ParallelConsumer<TxInputWithPrevOutputKey>(name + ".PrevTxLoader.2", threadCount);
        }

        public void Dispose()
        {
            this.txDispatcher.Dispose();
            this.txLoader.Dispose();

            if (this.pendingTxes != null)
                this.pendingTxes.Dispose();

            if (this.loadedTxes != null)
                this.loadedTxes.Dispose();
        }

        public int PendingCount { get { return this.txLoader.PendingCount; } }

        public IDisposable StartLoading(ConcurrentBlockingQueue<TxWithInputTxLookupKeys> pendingTxQueue)
        {
            this.loadingTxes = new ConcurrentDictionary<UInt256, Transaction[]>();

            this.pendingTxes = new ConcurrentBlockingQueue<TxInputWithPrevOutputKey>();
            this.loadedTxes = new ConcurrentBlockingQueue<LoadedTx>();

            this.txDispatcherStopper = StartTxDispatcher(pendingTxQueue);
            this.txLoaderStopper = StartTxLoader();

            return new DisposeAction(StopLoading);
        }

        public void WaitToComplete()
        {
            this.txDispatcher.WaitToComplete();
            this.txLoader.WaitToComplete();
        }

        public ConcurrentBlockingQueue<LoadedTx> GetQueue()
        {
            return this.loadedTxes;
        }

        private void StopLoading()
        {
            this.pendingTxes.CompleteAdding();
            this.loadedTxes.CompleteAdding();

            this.txDispatcherStopper.Dispose();
            this.txLoaderStopper.Dispose();
            this.pendingTxes.Dispose();
            this.loadedTxes.Dispose();

            this.loadingTxes = null;
            this.pendingTxes = null;
            this.loadedTxes = null;
            this.txDispatcherStopper = null;
            this.txLoaderStopper = null;
        }

        private IDisposable StartTxDispatcher(ConcurrentBlockingQueue<TxWithInputTxLookupKeys> pendingTxQueue)
        {
            return this.txDispatcher.Start(pendingTxQueue,
                pendingTx =>
                {
                    // queue up inputs to be loaded for non-coinbase transactions
                    if (pendingTx.TxIndex > 0)
                    {
                        // store an array for this transaction to fill in with inputs as they are loaded
                        if (!this.loadingTxes.TryAdd(pendingTx.Transaction.Hash, new Transaction[pendingTx.Transaction.Inputs.Length]))
                            throw new Exception("TODO");

                        this.pendingTxes.AddRange(pendingTx.GetInputs());
                    }
                    // no inputs to load for the coinbase transactions, queue it up immediately
                    else
                    {
                        this.loadedTxes.Add(new LoadedTx(pendingTx.Transaction, pendingTx.TxIndex, ImmutableArray.Create<Transaction>()));
                    }
                },
                _ => this.pendingTxes.CompleteAdding());
        }

        private IDisposable StartTxLoader()
        {
            return this.txLoader.Start(this.pendingTxes,
                pendingTx =>
                {
                    var loadedTx = LoadPendingTx(pendingTx);
                    if (loadedTx != null)
                        this.loadedTxes.Add(loadedTx);
                },
                _ => this.loadedTxes.CompleteAdding());
        }

        private LoadedTx LoadPendingTx(TxInputWithPrevOutputKey pendingTx)
        {
            var txIndex = pendingTx.TxIndex;
            var transaction = pendingTx.Transaction;
            var chainedHeader = pendingTx.ChainedHeader;
            var inputIndex = pendingTx.InputIndex;
            var prevOutputTxKey = pendingTx.PrevOutputTxKey;

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

            var inputPrevTxes = this.loadingTxes[transaction.Hash];
            bool completed;
            lock (inputPrevTxes)
            {
                inputPrevTxes[inputIndex] = inputPrevTx;
                completed = inputPrevTxes.All(x => x != null);
            }

            if (completed)
            {
                if (!this.loadingTxes.TryRemove(transaction.Hash, out inputPrevTxes))
                    throw new Exception("TODO");

                return new LoadedTx(transaction, txIndex, ImmutableArray.CreateRange(inputPrevTxes));
            }
            else
            {
                return null;
            }
        }
    }
}
