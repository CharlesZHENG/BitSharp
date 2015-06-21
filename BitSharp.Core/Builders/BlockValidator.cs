using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using NLog;
using System;
using System.Collections.Generic;
using System.Reactive;
using System.Reactive.Linq;

namespace BitSharp.Core.Builders
{
    internal class BlockValidator : IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly ChainStateBuilder.BuilderStats stats;
        private readonly CoreStorage coreStorage;
        private readonly IStorageManager storageManager;
        private readonly IBlockchainRules rules;

        private readonly ParallelObserver<LoadedTx> txValidator;
        private readonly ParallelObserver<Tuple<LoadedTx, int>> scriptValidator;

        public BlockValidator(ChainStateBuilder.BuilderStats stats, CoreStorage coreStorage, IBlockchainRules rules)
        {
            this.stats = stats;
            this.coreStorage = coreStorage;
            this.storageManager = coreStorage.StorageManager;
            this.rules = rules;

            // thread count for cpu tasks (TxValidator, ScriptValidator)
            var cpuThreadCount = Environment.ProcessorCount * 2;

            this.txValidator = new ParallelObserver<LoadedTx>("BlockValidator.TxValidator", cpuThreadCount);
            this.scriptValidator = new ParallelObserver<Tuple<LoadedTx, int>>("BlockValidator.ScriptValidator", cpuThreadCount);
        }

        public void Dispose()
        {
            this.txValidator.Dispose();
            this.scriptValidator.Dispose();
        }

        public void ValidateTransactions(ChainedHeader chainedHeader, IEnumerable<LoadedTx> loadedTxes, Action duringValidationAction = null)
        {
            using (var validateScriptQueue = new ConcurrentBlockingQueue<Tuple<LoadedTx, int>>())
            using (this.txValidator.SubscribeObservers(loadedTxes, StartTxValidator(chainedHeader, validateScriptQueue)))
            using (this.scriptValidator.SubscribeObservers(validateScriptQueue.GetConsumingEnumerable(), StartScriptValidator(chainedHeader)))
            {
                if (duringValidationAction != null)
                {
                    // wait for the loaded txes to have been fully queued up for validation
                    this.txValidator.WaitToCompleteReading();

                    // perform an action while validation is completing
                    duringValidationAction();
                }

                // wait for block validation to complete, any exceptions that ocurred will be thrown
                this.stats.waitToCompleteDurationMeasure.MeasureIf(chainedHeader.Height > 0, () =>
                    WaitToComplete(chainedHeader));
            }
        }

        private void WaitToComplete(ChainedHeader chainedHeader)
        {
            //this.prevTxLoader.WaitToComplete();
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
                    logger.Debug("Ignoring script errors in block: {0,9:#,##0}, errors: {1:#,##0}".Format2(chainedHeader.Height, e.InnerExceptions.Count));
            }
        }

        private IObserver<LoadedTx> StartTxValidator(ChainedHeader chainedHeader, ConcurrentBlockingQueue<Tuple<LoadedTx, int>> validateScriptQueue)
        {
            return Observer.Create<LoadedTx>(
                loadedTx =>
                {
                    QueueTransactionScripts(chainedHeader, loadedTx, validateScriptQueue);
                    this.rules.ValidateTransaction(chainedHeader, loadedTx);
                },
                ex => validateScriptQueue.CompleteAdding(),
                () => validateScriptQueue.CompleteAdding());
        }

        private IObserver<Tuple<LoadedTx, int>> StartScriptValidator(ChainedHeader chainedHeader)
        {
            return Observer.Create<Tuple<LoadedTx, int>>(
                tuple =>
                {
                    var loadedTx = tuple.Item1;
                    var inputIndex = tuple.Item2;
                    var txInput = loadedTx.Transaction.Inputs[inputIndex];
                    var prevTxOutput = loadedTx.GetInputPrevTxOutput(inputIndex);

                    this.rules.ValidationTransactionScript(chainedHeader, loadedTx.Transaction, loadedTx.TxIndex, txInput, inputIndex, prevTxOutput);
                });
        }

        private void QueueTransactionScripts(ChainedHeader chainedHeader, LoadedTx loadedTx, ConcurrentBlockingQueue<Tuple<LoadedTx, int>> validateScriptQueue)
        {
            if (!this.rules.IgnoreScripts)
            {
                var transaction = loadedTx.Transaction;
                var txIndex = loadedTx.TxIndex;

                if (!loadedTx.IsCoinbase)
                {
                    for (var inputIndex = 0; inputIndex < transaction.Inputs.Length; inputIndex++)
                        validateScriptQueue.Add(Tuple.Create(loadedTx, inputIndex));
                }
            }
        }
    }
}
