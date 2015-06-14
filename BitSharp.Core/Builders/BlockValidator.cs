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

        private readonly PrevTxLoader prevTxLoader;
        private readonly ParallelConsumer<TxWithPrevOutputs> txValidator;
        private readonly ParallelConsumer<TxInputWithPrevOutput> scriptValidator;

        private ConcurrentBlockingQueue<TxWithPrevOutputKeys> pendingTxQueue;
        private ConcurrentBlockingQueue<TxInputWithPrevOutput> validateScriptQueue;
        private IDisposable txLoaderStopper;
        private IDisposable txValidatorStopper;
        private IDisposable scriptValidatorStopper;
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

            this.prevTxLoader = new PrevTxLoader("BlockValidator", stats, coreStorage, ioThreadCount);
            this.txValidator = new ParallelConsumer<TxWithPrevOutputs>("BlockValidator.TxValidator", cpuThreadCount);
            this.scriptValidator = new ParallelConsumer<TxInputWithPrevOutput>("BlockValidator.ScriptValidator", cpuThreadCount);
        }

        public void Dispose()
        {
            this.prevTxLoader.Dispose();
            this.txValidator.Dispose();
            this.scriptValidator.Dispose();

            if (this.validateScriptQueue != null)
                this.validateScriptQueue.Dispose();
        }

        public int PendingPrevTxCount { get { return this.prevTxLoader.PendingCount; } }

        public ConcurrentBag<Exception> TxLoaderExceptions { get { return this.prevTxLoader.TxLoaderExceptions; } }

        public ConcurrentBag<Exception> TxValidatorExceptions { get { return this.txValidatorExceptions; } }

        public ConcurrentBag<Exception> ScriptValidatorExceptions { get { return this.scriptValidatorExceptions; } }

        public IDisposable StartValidation(ChainedHeader chainedHeader, ConcurrentBlockingQueue<TxWithPrevOutputKeys> pendingTxQueue)
        {
            this.pendingTxQueue = pendingTxQueue;
            this.validateScriptQueue = new ConcurrentBlockingQueue<TxInputWithPrevOutput>();

            this.txValidatorExceptions = new ConcurrentBag<Exception>();
            this.scriptValidatorExceptions = new ConcurrentBag<Exception>();

            this.txLoaderStopper = this.prevTxLoader.StartLoading(pendingTxQueue);
            this.txValidatorStopper = StartTxValidator(chainedHeader);
            this.scriptValidatorStopper = StartScriptValidator();

            return new DisposeAction(StopValidation);
        }

        public void WaitToComplete()
        {
            this.prevTxLoader.WaitToComplete();
            this.txValidator.WaitToComplete();
            this.scriptValidator.WaitToComplete();
        }

        private void StopValidation()
        {
            this.pendingTxQueue.CompleteAdding();
            this.validateScriptQueue.CompleteAdding();

            this.txLoaderStopper.Dispose();
            this.txValidatorStopper.Dispose();
            this.scriptValidatorStopper.Dispose();
            this.validateScriptQueue.Dispose();

            this.validateScriptQueue = null;
            this.txLoaderStopper = null;
            this.txValidatorStopper = null;
            this.scriptValidatorStopper = null;
            this.txValidatorExceptions = null;
            this.scriptValidatorExceptions = null;
        }

        private IDisposable StartTxValidator(ChainedHeader chainedHeader)
        {
            return this.txValidator.Start(this.prevTxLoader.GetQueue(),
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
                                this.validateScriptQueue.Add(txInputWithPrevOutput);
                            }
                        }
                    }

                    ValidateTransaction(loadedTx);
                },
                _ => this.validateScriptQueue.CompleteAdding());
        }

        private IDisposable StartScriptValidator()
        {
            return this.scriptValidator.Start(this.validateScriptQueue,
                loadedTxInput =>
                {
                    ValidateScript(loadedTxInput);
                },
                _ => { });
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
    }
}
