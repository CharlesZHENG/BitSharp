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
using System.Threading.Tasks;

namespace BitSharp.Core.Builders
{
    internal class BlockValidator : IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly CoreStorage coreStorage;
        private readonly IBlockchainRules rules;

        private readonly ParallelObserver<LoadedTx> txValidator;
        private readonly ParallelObserver<Tuple<LoadedTx, int>> scriptValidator;
        private TaskCompletionSource<object> tcs;
        private ConcurrentBlockingQueue<Tuple<LoadedTx, int>> validateScriptQueue;
        private bool scriptError;

        public BlockValidator(CoreStorage coreStorage, IBlockchainRules rules)
        {
            this.coreStorage = coreStorage;
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

        public Task ValidateTransactions(ChainedHeader chainedHeader, IEnumerable<LoadedTx> loadedTxes, out Task loadedTxesReadTask)
        {
            this.tcs = new TaskCompletionSource<object>();
            this.validateScriptQueue = new ConcurrentBlockingQueue<Tuple<LoadedTx, int>>();
            this.scriptError = false;

            var txValidatorTask = this.txValidator.SubscribeObservers(loadedTxes, CreateTxValidator(chainedHeader), out loadedTxesReadTask);
            var scriptValidatorTask = this.scriptValidator.SubscribeObservers(validateScriptQueue.GetConsumingEnumerable(), CreateScriptValidator(chainedHeader, txValidatorTask));

            return this.tcs.Task;
        }

        private IObserver<LoadedTx> CreateTxValidator(ChainedHeader chainedHeader)
        {
            return Observer.Create<LoadedTx>(
                loadedTx =>
                {
                    QueueTransactionScripts(chainedHeader, loadedTx);
                    this.rules.ValidateTransaction(chainedHeader, loadedTx);
                },
                ex => validateScriptQueue.CompleteAdding(),
                () => validateScriptQueue.CompleteAdding());
        }

        private IObserver<Tuple<LoadedTx, int>> CreateScriptValidator(ChainedHeader chainedHeader, Task txValidatorTask)
        {
            return Observer.Create<Tuple<LoadedTx, int>>(
                tuple =>
                {
                    var loadedTx = tuple.Item1;
                    var inputIndex = tuple.Item2;
                    var txInput = loadedTx.Transaction.Inputs[inputIndex];
                    var prevTxOutput = loadedTx.GetInputPrevTxOutput(inputIndex);

                    var success = false;
                    try
                    {
                        this.rules.ValidationTransactionScript(chainedHeader, loadedTx.Transaction, loadedTx.TxIndex, txInput, inputIndex, prevTxOutput);
                        success = true;
                    }
                    finally
                    {
                        if (!success)
                            this.scriptError = true;
                    }
                },
                ex => Finish(chainedHeader, txValidatorTask, ex),
                () => Finish(chainedHeader, txValidatorTask));
        }

        private void QueueTransactionScripts(ChainedHeader chainedHeader, LoadedTx loadedTx)
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

        private void Finish(ChainedHeader chainedHeader, Task txValidatorTask, Exception ex = null)
        {
            try
            {
                txValidatorTask.Wait();
            }
            catch (Exception txValidatorEx)
            {
                ex = txValidatorEx;
            }
            var validateScriptQueueLocal = this.validateScriptQueue;

            bool finished;
            if (ex != null && this.scriptError && this.rules.IgnoreScriptErrors)
            {
                logger.Debug("Ignoring script errors in block: {0,9:#,##0}, errors: {1:#,##0}".Format2(chainedHeader.Height, ((AggregateException)ex).InnerExceptions.Count));
                finished = tcs.TrySetResult(null);
            }
            else if (ex != null)
                finished = tcs.TrySetException(ex);
            else
                finished = tcs.TrySetResult(null);

            if (finished)
            {
                validateScriptQueueLocal.CompleteAdding();
                validateScriptQueueLocal.Dispose();
            }
        }
    }
}
