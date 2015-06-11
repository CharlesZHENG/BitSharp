using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;

namespace BitSharp.Core.Builders
{
    internal class BlockValidator : IDisposable
    {
        private readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly ChainStateBuilder.BuilderStats stats;
        private readonly CoreStorage coreStorage;
        private readonly IStorageManager storageManager;
        private readonly IBlockchainRules rules;

        private readonly ParallelConsumer<TxInputWithPrevOutputKey> txLoader;
        private readonly ParallelConsumer<TxWithPrevOutputs> txValidator;
        private readonly ParallelConsumer<TxInputWithPrevOutput> scriptValidator;

        private ConcurrentDictionary<UInt256, TxOutput[]> loadingTxes;
        private ConcurrentBlockingQueue<TxInputWithPrevOutputKey> pendingTxes;
        private ConcurrentBlockingQueue<TxWithPrevOutputs> loadedTxes;
        private ConcurrentBlockingQueue<TxInputWithPrevOutput> loadedTxInputs;
        private IDisposable txLoaderStopper;
        private IDisposable txValidatorStopper;
        private IDisposable scriptValidatorStopper;
        private ConcurrentBag<Exception> txLoaderExceptions;
        private ConcurrentBag<Exception> txValidatorExceptions;
        private ConcurrentBag<Exception> scriptValidatorExceptions;

        public BlockValidator(ChainStateBuilder.BuilderStats stats, CoreStorage coreStorage, IBlockchainRules rules)
        {
            this.stats = stats;
            this.coreStorage = coreStorage;
            this.storageManager = coreStorage.StorageManager;
            this.rules = rules;

            // thread count for i/o task (TxLoader)
            var ioThreadCount = Environment.ProcessorCount * 2;

            // thread count for cpu tasks (TxValidator, ScriptValidator)
            var cpuThreadCount = Environment.ProcessorCount * 2;

            this.txLoader = new ParallelConsumer<TxInputWithPrevOutputKey>("BlockValidator.TxLoader", ioThreadCount);
            this.txValidator = new ParallelConsumer<TxWithPrevOutputs>("BlockValidator.TxValidator", cpuThreadCount);
            this.scriptValidator = new ParallelConsumer<TxInputWithPrevOutput>("BlockValidator.ScriptValidator", cpuThreadCount);
        }

        public void Dispose()
        {
            this.txLoader.Dispose();
            this.txValidator.Dispose();
            this.scriptValidator.Dispose();

            if (this.pendingTxes != null)
                this.pendingTxes.Dispose();

            if (this.loadedTxes != null)
                this.loadedTxes.Dispose();

            if (this.loadedTxInputs != null)
                this.loadedTxInputs.Dispose();
        }

        public ConcurrentBag<Exception> TxLoaderExceptions { get { return this.txLoaderExceptions; } }

        public ConcurrentBag<Exception> TxValidatorExceptions { get { return this.txValidatorExceptions; } }

        public ConcurrentBag<Exception> ScriptValidatorExceptions { get { return this.scriptValidatorExceptions; } }

        public IDisposable StartValidation(ChainedHeader chainedHeader)
        {
            this.loadingTxes = new ConcurrentDictionary<UInt256, TxOutput[]>();

            this.pendingTxes = new ConcurrentBlockingQueue<TxInputWithPrevOutputKey>();
            this.loadedTxes = new ConcurrentBlockingQueue<TxWithPrevOutputs>();
            this.loadedTxInputs = new ConcurrentBlockingQueue<TxInputWithPrevOutput>();

            this.txLoaderExceptions = new ConcurrentBag<Exception>();
            this.txValidatorExceptions = new ConcurrentBag<Exception>();
            this.scriptValidatorExceptions = new ConcurrentBag<Exception>();

            this.txLoaderStopper = StartTxLoader();
            this.txValidatorStopper = StartTxValidator(chainedHeader);
            this.scriptValidatorStopper = StartScriptValidator();

            return new Stopper(this);
        }

        public void AddPendingTx(TxWithPrevOutputKeys pendingTx)
        {
            if (pendingTx.TxIndex > 0)
            {
                if (!this.loadingTxes.TryAdd(pendingTx.Transaction.Hash, new TxOutput[pendingTx.Transaction.Inputs.Length]))
                    throw new Exception("TODO");
            }

            this.pendingTxes.AddRange(pendingTx.GetInputs());
        }

        public void CompleteAdding()
        {
            this.pendingTxes.CompleteAdding();
            this.stats.pendingTxesAtCompleteAverageMeasure.Tick(this.txLoader.PendingCount);
        }

        public void WaitToComplete()
        {
            this.txLoader.WaitToComplete();
            this.txValidator.WaitToComplete();
            this.scriptValidator.WaitToComplete();
        }

        private void StopValidation()
        {
            this.pendingTxes.CompleteAdding();
            this.loadedTxes.CompleteAdding();
            this.loadedTxInputs.CompleteAdding();

            this.txLoaderStopper.Dispose();
            this.txValidatorStopper.Dispose();
            this.scriptValidatorStopper.Dispose();
            this.pendingTxes.Dispose();
            this.loadedTxes.Dispose();
            this.loadedTxInputs.Dispose();

            this.loadingTxes = null;
            this.pendingTxes = null;
            this.loadedTxes = null;
            this.loadedTxInputs = null;
            this.txLoaderStopper = null;
            this.txValidatorStopper = null;
            this.scriptValidatorStopper = null;
            this.txLoaderExceptions = null;
            this.txValidatorExceptions = null;
            this.scriptValidatorExceptions = null;
        }

        private IDisposable StartTxLoader()
        {
            return this.txLoader.Start(this.pendingTxes,
                pendingTx =>
                {
                    if (this.rules.BypassValidation)
                        return;

                    var loadedTx = LoadPendingTx(pendingTx);
                    if (loadedTx != null)
                        this.loadedTxes.Add(loadedTx);
                },
                _ => this.loadedTxes.CompleteAdding());
        }

        private IDisposable StartTxValidator(ChainedHeader chainedHeader)
        {
            return this.txValidator.Start(this.loadedTxes,
                loadedTx =>
                {
                    if (!this.rules.IgnoreScripts)
                    {
                        var transaction = loadedTx.Transaction;
                        var txIndex = loadedTx.TxIndex;
                        var prevTxOutputs = loadedTx.PrevTxOutputs;

                        if (txIndex > 0)
                        {
                            for (var inputIndex = 0; inputIndex < transaction.Inputs.Length; inputIndex++)
                            {
                                var txInput = transaction.Inputs[inputIndex];
                                var prevTxOutput = prevTxOutputs[inputIndex];

                                var txInputWithPrevOutput = new TxInputWithPrevOutput(chainedHeader, transaction, txIndex, txInput, inputIndex, prevTxOutput);
                                this.loadedTxInputs.Add(txInputWithPrevOutput);
                            }
                        }
                    }

                    ValidateTransaction(loadedTx);
                },
                _ => this.loadedTxInputs.CompleteAdding());
        }

        private IDisposable StartScriptValidator()
        {
            return this.scriptValidator.Start(this.loadedTxInputs,
                loadedTxInput =>
                {
                    ValidateScript(loadedTxInput);
                },
                _ => { });
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
                        this.stats.prevTxLoadDurationMeasure.Tick(stopwatch.Elapsed);
                        this.stats.prevTxLoadRateMeasure.Tick();

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

        private void ValidateTransaction(TxWithPrevOutputs loadedTx)
        {
            try
            {
                var chainedHeader = loadedTx.ChainedHeader;
                var transaction = loadedTx.Transaction;
                var txIndex = loadedTx.TxIndex;
                var prevTxOutputs = loadedTx.PrevTxOutputs;

                this.rules.ValidateTransaction(chainedHeader, transaction, txIndex, prevTxOutputs);
            }
            catch (Exception e)
            {
                this.txValidatorExceptions.Add(e);
            }
        }

        private void ValidateScript(TxInputWithPrevOutput loadedTxInput)
        {
            try
            {
                this.rules.ValidationTransactionScript(loadedTxInput.ChainedHeader, loadedTxInput.Transaction, loadedTxInput.TxIndex, loadedTxInput.TxInput, loadedTxInput.InputIndex, loadedTxInput.PrevTxOutput);
            }
            catch (Exception e)
            {
                this.scriptValidatorExceptions.Add(e);
            }
        }

        private sealed class Stopper : IDisposable
        {
            private readonly BlockValidator blockValidator;

            public Stopper(BlockValidator blockValidator)
            {
                this.blockValidator = blockValidator;
            }

            public void Dispose()
            {
                this.blockValidator.StopValidation();
            }
        }
    }
}
