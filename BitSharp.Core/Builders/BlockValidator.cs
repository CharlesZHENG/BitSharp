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
        private readonly ParallelConsumer<LoadedTx> txValidator;
        private readonly ParallelConsumer<TxInputWithPrevOutput> scriptValidator;

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
            this.txValidator = new ParallelConsumer<LoadedTx>("BlockValidator.TxValidator", cpuThreadCount);
            this.scriptValidator = new ParallelConsumer<TxInputWithPrevOutput>("BlockValidator.ScriptValidator", cpuThreadCount);
        }

        public void Dispose()
        {
            this.prevTxLoader.Dispose();
            this.txValidator.Dispose();
            this.scriptValidator.Dispose();
        }

        public void ValidateTransactions(ChainedHeader chainedHeader, Action<ConcurrentBlockingQueue<TxWithInputTxLookupKeys>> workAction)
        {
            using (var pendingTxQueue = new ConcurrentBlockingQueue<TxWithInputTxLookupKeys>())
            using (var validateScriptQueue = new ConcurrentBlockingQueue<TxInputWithPrevOutput>())
            using (var txLoaderStopper = this.prevTxLoader.StartLoading(pendingTxQueue))
            using (var txValidatorStopper = StartTxValidator(chainedHeader, validateScriptQueue))
            using (var scriptValidatorStopper = StartScriptValidator(validateScriptQueue))
            {
                try
                {
                    workAction(pendingTxQueue);
                }
                finally
                {
                    pendingTxQueue.CompleteAdding();
                }

                this.stats.pendingTxesAtCompleteAverageMeasure.Tick(this.prevTxLoader.PendingCount);

                // wait for block validation to complete, any exceptions that ocurred will be thrown
                this.stats.waitToCompleteDurationMeasure.Measure(() =>
                    WaitToComplete(chainedHeader));
            }
        }

        private void WaitToComplete(ChainedHeader chainedHeader)
        {
            this.prevTxLoader.WaitToComplete();
            this.txValidator.WaitToComplete();
            //TODO remove IgnoreScriptErrors
            try
            {
                this.scriptValidator.WaitToComplete();
            }
            catch (AggregateException e)
            {
                if (!this.rules.IgnoreScriptErrors)
                    throw;
                else
                    this.logger.Debug("Ignoring script errors in block: {0,9:#,##0}, errors: {1:#,##0}".Format2(chainedHeader.Height, e.InnerExceptions.Count));
            }
        }

        private IDisposable StartTxValidator(ChainedHeader chainedHeader, ConcurrentBlockingQueue<TxInputWithPrevOutput> validateScriptQueue)
        {
            return this.txValidator.Start(this.prevTxLoader.GetQueue(),
                loadedTx =>
                {
                    QueueTransactionScripts(chainedHeader, loadedTx, validateScriptQueue);
                    this.rules.ValidateTransaction(chainedHeader, loadedTx);
                },
                _ => validateScriptQueue.CompleteAdding());
        }

        private IDisposable StartScriptValidator(ConcurrentBlockingQueue<TxInputWithPrevOutput> validateScriptQueue)
        {
            return this.scriptValidator.Start(validateScriptQueue,
                loadedTxInput =>
                {
                    this.rules.ValidationTransactionScript(loadedTxInput.ChainedHeader, loadedTxInput.Transaction, loadedTxInput.TxIndex, loadedTxInput.TxInput, loadedTxInput.InputIndex, loadedTxInput.PrevTxOutput);
                },
                _ => { });
        }

        private void QueueTransactionScripts(ChainedHeader chainedHeader, LoadedTx loadedTx, ConcurrentBlockingQueue<TxInputWithPrevOutput> validateScriptQueue)
        {
            if (!this.rules.IgnoreScripts)
            {
                var transaction = loadedTx.Transaction;
                var txIndex = loadedTx.TxIndex;

                if (txIndex > 0)
                {
                    for (var inputIndex = 0; inputIndex < transaction.Inputs.Length; inputIndex++)
                    {
                        var txInput = transaction.Inputs[inputIndex];
                        var prevTxOutput = loadedTx.GetInputPrevTxOutput(inputIndex);

                        var txInputWithPrevOutput = new TxInputWithPrevOutput(chainedHeader, transaction, txIndex, txInput, inputIndex, prevTxOutput);
                        validateScriptQueue.Add(txInputWithPrevOutput);
                    }
                }
            }
        }
    }
}
