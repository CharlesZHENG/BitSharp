using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using NLog;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reactive;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Core.Builders
{
    internal class BlockValidator : IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly ReaderWriterLockSlim controlLock = new ReaderWriterLockSlim();

        private readonly CoreStorage coreStorage;
        private readonly IBlockchainRules rules;

        private readonly ParallelConsumerProducer<LoadedTx, Tuple<LoadedTx, int>> txValidator;
        private readonly ParallelObserver<Tuple<LoadedTx, int>> scriptValidator;

        private TaskCompletionSource<object> tcs;
        private ConcurrentBlockingQueue<Tuple<LoadedTx, int>> validateScriptQueue;
        private bool scriptError;
        private Exception taskException;

        public BlockValidator(CoreStorage coreStorage, IBlockchainRules rules)
        {
            this.coreStorage = coreStorage;
            this.rules = rules;

            // thread count for cpu tasks (TxValidator, ScriptValidator)
            var cpuThreadCount = Environment.ProcessorCount * 2;

            this.txValidator = new ParallelConsumerProducer<LoadedTx, Tuple<LoadedTx, int>>("BlockValidator.TxValidator", cpuThreadCount);
            this.scriptValidator = new ParallelObserver<Tuple<LoadedTx, int>>("BlockValidator.ScriptValidator", cpuThreadCount);
        }

        public void Dispose()
        {
            this.txValidator.Dispose();
            this.scriptValidator.Dispose();
            this.controlLock.Dispose();
        }

        public Task ValidateTransactions(ChainedHeader chainedHeader, ParallelReader<LoadedTx> loadedTxes)
        {
            controlLock.EnterWriteLock();
            try
            {
                if (tcs != null)
                    throw new InvalidOperationException();

                this.tcs = new TaskCompletionSource<object>();
                this.validateScriptQueue = new ConcurrentBlockingQueue<Tuple<LoadedTx, int>>();
                this.scriptError = false;
                this.taskException = null;

                var txValidatorTask = this.txValidator.ConsumeProduceAsync(loadedTxes,
                    CreateTxValidator(chainedHeader),
                    loadedTx => QueueTransactionScripts(chainedHeader, loadedTx));

                var scriptValidatorTask = this.scriptValidator.SubscribeObservers(txValidator, CreateScriptValidator(chainedHeader, txValidatorTask));

                return this.tcs.Task;
            }
            finally
            {
                controlLock.ExitWriteLock();
            }
        }

        private IObserver<LoadedTx> CreateTxValidator(ChainedHeader chainedHeader)
        {
            return Observer.Create<LoadedTx>(
                loadedTx =>
                {
                    QueueTransactionScripts(chainedHeader, loadedTx);
                    this.rules.ValidateTransaction(chainedHeader, loadedTx);
                },
                ex => { taskException = taskException ?? ex; validateScriptQueue.CompleteAdding(); },
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

                    try
                    {
                        this.rules.ValidationTransactionScript(chainedHeader, loadedTx.Transaction, loadedTx.TxIndex, txInput, inputIndex, prevTxOutput);
                    }
                    catch (Exception ex)
                    {
                        taskException = ex;
                        scriptError = true;
                    }
                },
                ex => Finish(chainedHeader, txValidatorTask, ex),
                () => Finish(chainedHeader, txValidatorTask));
        }

        private IEnumerable<Tuple<LoadedTx, int>> QueueTransactionScripts(ChainedHeader chainedHeader, LoadedTx loadedTx)
        {
            if (!this.rules.IgnoreScripts)
            {
                var transaction = loadedTx.Transaction;
                var txIndex = loadedTx.TxIndex;

                if (!loadedTx.IsCoinbase)
                {
                    for (var inputIndex = 0; inputIndex < transaction.Inputs.Length; inputIndex++)
                        yield return Tuple.Create(loadedTx, inputIndex);
                }
            }
        }

        private void Finish(ChainedHeader chainedHeader, Task txValidatorTask, Exception ex = null)
        {
            controlLock.EnterUpgradeableReadLock();
            try
            {
                taskException = taskException ?? ex;
                try
                {
                    txValidatorTask.Wait();
                }
                catch (Exception validatorEx)
                {
                    taskException = taskException ?? validatorEx;
                }

                controlLock.EnterWriteLock();
                try
                {
                    if (taskException != null)
                    {
                        if (this.scriptError && this.rules.IgnoreScriptErrors)
                        {
                            var aggEx = taskException as AggregateException;
                            logger.Debug("Ignoring script errors in block: {0,9:#,##0}, errors: {1:#,##0}".Format2(chainedHeader.Height, aggEx != null ? aggEx.InnerExceptions.Count : -1));
                            tcs.SetResult(null);
                        }
                        else
                            tcs.SetException(taskException);
                    }
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
