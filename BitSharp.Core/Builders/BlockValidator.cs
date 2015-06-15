using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using NLog;
using System;

namespace BitSharp.Core.Builders
{
    internal class BlockValidator : IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly ChainStateBuilder.BuilderStats stats;
        private readonly CoreStorage coreStorage;
        private readonly IStorageManager storageManager;
        private readonly IBlockchainRules rules;

        private readonly TxLoader txLoader;
        private readonly ParallelConsumer<LoadedTx> txValidator;
        private readonly ParallelConsumer<Tuple<LoadedTx, int>> scriptValidator;

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

            this.txLoader = new TxLoader("BlockValidator", stats, coreStorage, ioThreadCount);
            this.txValidator = new ParallelConsumer<LoadedTx>("BlockValidator.TxValidator", cpuThreadCount);
            this.scriptValidator = new ParallelConsumer<Tuple<LoadedTx, int>>("BlockValidator.ScriptValidator", cpuThreadCount);
        }

        public void Dispose()
        {
            this.txLoader.Dispose();
            this.txValidator.Dispose();
            this.scriptValidator.Dispose();
        }

        public void ValidateTransactions(ChainedHeader chainedHeader, Action<ConcurrentBlockingQueue<LoadingTx>> workAction)
        {
            using (var loadingTxes = new ConcurrentBlockingQueue<LoadingTx>())
            {
                this.txLoader.LoadTxes(loadingTxes,
                    loadedTxes =>
                    {
                        using (var validateScriptQueue = new ConcurrentBlockingQueue<Tuple<LoadedTx, int>>())
                        using (StartTxValidator(chainedHeader, loadedTxes, validateScriptQueue))
                        using (StartScriptValidator(chainedHeader, validateScriptQueue))
                        {
                            try
                            {
                                workAction(loadingTxes);
                            }
                            finally
                            {
                                loadingTxes.CompleteAdding();
                            }

                            this.stats.pendingTxesAtCompleteAverageMeasure.Tick(this.txLoader.PendingCount);

                            // wait for block validation to complete, any exceptions that ocurred will be thrown
                            this.stats.waitToCompleteDurationMeasure.Measure(() =>
                                WaitToComplete(chainedHeader));
                        }
                    });
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

        private IDisposable StartTxValidator(ChainedHeader chainedHeader, ConcurrentBlockingQueue<LoadedTx> loadedTxes, ConcurrentBlockingQueue<Tuple<LoadedTx, int>> validateScriptQueue)
        {
            return this.txValidator.Start(loadedTxes,
                loadedTx =>
                {
                    QueueTransactionScripts(chainedHeader, loadedTx, validateScriptQueue);
                    this.rules.ValidateTransaction(chainedHeader, loadedTx);
                },
                _ => validateScriptQueue.CompleteAdding());
        }

        private IDisposable StartScriptValidator(ChainedHeader chainedHeader, ConcurrentBlockingQueue<Tuple<LoadedTx, int>> validateScriptQueue)
        {
            return this.scriptValidator.Start(validateScriptQueue,
                loadedTxInput =>
                {
                    var loadedTx = loadedTxInput.Item1;
                    var inputIndex = loadedTxInput.Item2;
                    var txInput = loadedTx.Transaction.Inputs[inputIndex];
                    var prevTxOutput = loadedTx.GetInputPrevTxOutput(inputIndex);

                    this.rules.ValidationTransactionScript(chainedHeader, loadedTx.Transaction, loadedTx.TxIndex, txInput, inputIndex, prevTxOutput);
                },
                _ => { });
        }

        private void QueueTransactionScripts(ChainedHeader chainedHeader, LoadedTx loadedTx, ConcurrentBlockingQueue<Tuple<LoadedTx, int>> validateScriptQueue)
        {
            if (!this.rules.IgnoreScripts)
            {
                var transaction = loadedTx.Transaction;
                var txIndex = loadedTx.TxIndex;

                if (txIndex > 0)
                {
                    for (var inputIndex = 0; inputIndex < transaction.Inputs.Length; inputIndex++)
                        validateScriptQueue.Add(Tuple.Create(loadedTx, inputIndex));
                }
            }
        }
    }
}
