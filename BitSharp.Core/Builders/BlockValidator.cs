using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reactive;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BitSharp.Core.Builders
{
    internal class BlockValidator : IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly CoreStorage coreStorage;
        private readonly IBlockchainRules rules;
        private readonly ChainedHeader chainedHeader;
        private readonly ISourceBlock<LoadedTx> loadedTxes;

        private readonly CancellationTokenSource cancelToken = new CancellationTokenSource();
        private Task completion;

        private TransformManyBlock<LoadedTx, Tuple<LoadedTx, int>> txValidator;
        private ActionBlock<Tuple<LoadedTx, int>> scriptValidator;

        public BlockValidator(CoreStorage coreStorage, IBlockchainRules rules, ChainedHeader chainedHeader, ISourceBlock<LoadedTx> loadedTxes)
        {
            this.coreStorage = coreStorage;
            this.rules = rules;
            this.chainedHeader = chainedHeader;
            this.loadedTxes = loadedTxes;

            completion = Task.Factory.StartNew(async () => await ValidateTransactions(), cancelToken.Token, TaskCreationOptions.AttachedToParent, TaskScheduler.Default);
        }

        public void Dispose()
        {
            cancelToken.Dispose();
        }

        public Task Completion { get { return completion; } }

        private async Task ValidateTransactions()
        {
            // validate transactions
            txValidator = InitTxValidator();

            // validate scripts
            scriptValidator = InitScriptValidator();

            // begin feeding the tx validator
            loadedTxes.LinkTo(txValidator, new DataflowLinkOptions { PropagateCompletion = true });

            // begin feeding the script validator
            txValidator.LinkTo(scriptValidator, new DataflowLinkOptions { PropagateCompletion = true });

            await txValidator.Completion;
            await scriptValidator.Completion;
        }

        private TransformManyBlock<LoadedTx, Tuple<LoadedTx, int>> InitTxValidator()
        {
            return new TransformManyBlock<LoadedTx, Tuple<LoadedTx, int>>(
                loadedTx =>
                {
                    rules.ValidateTransaction(chainedHeader, loadedTx);

                    if (!this.rules.IgnoreScripts && !loadedTx.IsCoinbase)
                    {
                        var scripts = new Tuple<LoadedTx, int>[loadedTx.Transaction.Inputs.Length];
                        for (var i = 0; i < loadedTx.Transaction.Inputs.Length; i++)
                            scripts[i] = Tuple.Create(loadedTx, i);

                        return scripts;
                    }
                    else
                        return new Tuple<LoadedTx, int>[0];
                },
                new ExecutionDataflowBlockOptions { CancellationToken = cancelToken.Token, MaxDegreeOfParallelism = 16 });
        }

        private ActionBlock<Tuple<LoadedTx, int>> InitScriptValidator()
        {
            return new ActionBlock<Tuple<LoadedTx, int>>(
                tuple =>
                {
                    var loadedTx = tuple.Item1;
                    var inputIndex = tuple.Item2;
                    var txInput = loadedTx.Transaction.Inputs[inputIndex];
                    var prevTxOutput = loadedTx.GetInputPrevTxOutput(inputIndex);

                    if (!this.rules.IgnoreScriptErrors)
                    {
                        this.rules.ValidationTransactionScript(chainedHeader, loadedTx.Transaction, loadedTx.TxIndex, txInput, inputIndex, prevTxOutput);
                    }
                    else
                    {
                        try
                        {
                            this.rules.ValidationTransactionScript(chainedHeader, loadedTx.Transaction, loadedTx.TxIndex, txInput, inputIndex, prevTxOutput);
                        }
                        catch (Exception ex)
                        {
                            var aggEx = ex as AggregateException;
                            logger.Debug("Ignoring script errors in block: {0,9:#,##0}, errors: {1:#,##0}".Format2(chainedHeader.Height, aggEx != null ? aggEx.InnerExceptions.Count : -1));
                        }
                    }
                },
                new ExecutionDataflowBlockOptions { CancellationToken = cancelToken.Token, MaxDegreeOfParallelism = 16 });
        }
    }
}
