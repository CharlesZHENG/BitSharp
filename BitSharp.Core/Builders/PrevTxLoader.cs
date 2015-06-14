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

        private readonly ParallelConsumer<TxWithPrevOutputKeys> txDispatcher;
        private readonly ParallelConsumer<TxInputWithPrevOutputKey> txLoader;

        private ConcurrentDictionary<UInt256, TxOutput[]> loadingTxes;
        private ConcurrentBlockingQueue<TxInputWithPrevOutputKey> pendingTxes;
        private ConcurrentBlockingQueue<TxWithPrevOutputs> loadedTxes;
        private ConcurrentBlockingQueue<TxInputWithPrevOutput> loadedTxInputs;
        private IDisposable txDispatcherStopper;
        private IDisposable txLoaderStopper;
        private ConcurrentBag<Exception> txLoaderExceptions;

        public PrevTxLoader(string name, ChainStateBuilder.BuilderStats stats, CoreStorage coreStorage, int threadCount)
        {
            this.stats = stats;
            this.coreStorage = coreStorage;

            this.txDispatcher = new ParallelConsumer<TxWithPrevOutputKeys>(name + ".PrevTxLoader.1", 1);
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

            if (this.loadedTxInputs != null)
                this.loadedTxInputs.Dispose();
        }

        public int PendingCount { get { return this.txLoader.PendingCount; } }

        public ConcurrentBag<Exception> TxLoaderExceptions { get { return this.txLoaderExceptions; } }

        public IDisposable StartLoading(ConcurrentBlockingQueue<TxWithPrevOutputKeys> pendingTxQueue)
        {
            this.loadingTxes = new ConcurrentDictionary<UInt256, TxOutput[]>();

            this.pendingTxes = new ConcurrentBlockingQueue<TxInputWithPrevOutputKey>();
            this.loadedTxes = new ConcurrentBlockingQueue<TxWithPrevOutputs>();
            this.loadedTxInputs = new ConcurrentBlockingQueue<TxInputWithPrevOutput>();

            this.txLoaderExceptions = new ConcurrentBag<Exception>();

            this.txDispatcherStopper = StartTxDispatcher(pendingTxQueue);
            this.txLoaderStopper = StartTxLoader();

            return new DisposeAction(StopLoading);
        }

        public void WaitToComplete()
        {
            this.txLoader.WaitToComplete();
        }

        public ConcurrentBlockingQueue<TxWithPrevOutputs> GetQueue()
        {
            return this.loadedTxes;
        }

        private void StopLoading()
        {
            this.pendingTxes.CompleteAdding();
            this.loadedTxes.CompleteAdding();
            this.loadedTxInputs.CompleteAdding();

            this.txDispatcherStopper.Dispose();
            this.txLoaderStopper.Dispose();
            this.pendingTxes.Dispose();
            this.loadedTxes.Dispose();
            this.loadedTxInputs.Dispose();

            this.loadingTxes = null;
            this.pendingTxes = null;
            this.loadedTxes = null;
            this.loadedTxInputs = null;
            this.txDispatcherStopper = null;
            this.txLoaderStopper = null;
            this.txLoaderExceptions = null;
        }

        private IDisposable StartTxDispatcher(ConcurrentBlockingQueue<TxWithPrevOutputKeys> pendingTxQueue)
        {
            return this.txDispatcher.Start(pendingTxQueue,
                pendingTx =>
                {
                    if (pendingTx.TxIndex > 0)
                    {
                        if (!this.loadingTxes.TryAdd(pendingTx.Transaction.Hash, new TxOutput[pendingTx.Transaction.Inputs.Length]))
                            throw new Exception("TODO");
                    }

                    this.pendingTxes.AddRange(pendingTx.GetInputs());
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

        private TxWithPrevOutputs LoadPendingTx(TxInputWithPrevOutputKey pendingTx)
        {
            try
            {
                var txIndex = pendingTx.TxIndex;
                var transaction = pendingTx.Transaction;
                var chainedHeader = pendingTx.ChainedHeader;
                var inputIndex = pendingTx.InputIndex;
                var prevOutputTxKey = pendingTx.PrevOutputTxKey;

                // load previous transactions for each input, unless this is a coinbase transaction
                if (txIndex > 0)
                {
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

                    var prevTxOutput = inputPrevTx.Outputs[input.PreviousTxOutputKey.TxOutputIndex.ToIntChecked()];

                    var prevTxOutputs = this.loadingTxes[transaction.Hash];
                    bool completed;
                    lock (prevTxOutputs)
                    {
                        prevTxOutputs[inputIndex] = prevTxOutput;
                        completed = prevTxOutputs.All(x => x != null);
                    }

                    if (completed)
                    {
                        if (!this.loadingTxes.TryRemove(transaction.Hash, out prevTxOutputs))
                            throw new Exception("TODO");

                        return new TxWithPrevOutputs(txIndex, transaction, chainedHeader, ImmutableArray.CreateRange(prevTxOutputs));
                    }
                    else
                    {
                        return null;
                    }
                }
                else
                {
                    if (inputIndex == 0)
                        return new TxWithPrevOutputs(txIndex, transaction, chainedHeader, ImmutableArray.Create<TxOutput>());
                    else
                        return null;
                }
            }
            catch (Exception e)
            {
                this.txLoaderExceptions.Add(e);
                //TODO
                return null;
            }
        }
    }
}
