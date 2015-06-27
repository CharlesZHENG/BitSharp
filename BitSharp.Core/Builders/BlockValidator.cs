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
    internal static class BlockValidator
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        public static async Task ValidateBlock(CoreStorage coreStorage, IBlockchainRules rules, ChainedHeader chainedHeader, ISourceBlock<LoadedTx> loadedTxes, CancellationToken cancelToken = default(CancellationToken))
        {
            // validate transactions
            var txValidator = InitTxValidator(rules, chainedHeader, cancelToken);

            // validate scripts
            var scriptValidator = InitScriptValidator(rules, chainedHeader, cancelToken);

            // begin feeding the tx validator
            loadedTxes.LinkTo(txValidator, new DataflowLinkOptions { PropagateCompletion = true });

            // begin feeding the script validator
            txValidator.LinkTo(scriptValidator, new DataflowLinkOptions { PropagateCompletion = true });

            await txValidator.Completion;
            await scriptValidator.Completion;
        }

        private static TransformManyBlock<LoadedTx, Tuple<LoadedTx, int>> InitTxValidator(IBlockchainRules rules, ChainedHeader chainedHeader, CancellationToken cancelToken)
        {
            return new TransformManyBlock<LoadedTx, Tuple<LoadedTx, int>>(
                loadedTx =>
                {
                    rules.ValidateTransaction(chainedHeader, loadedTx);

                    if (!rules.IgnoreScripts && !loadedTx.IsCoinbase)
                    {
                        var scripts = new Tuple<LoadedTx, int>[loadedTx.Transaction.Inputs.Length];
                        for (var i = 0; i < loadedTx.Transaction.Inputs.Length; i++)
                            scripts[i] = Tuple.Create(loadedTx, i);

                        return scripts;
                    }
                    else
                        return new Tuple<LoadedTx, int>[0];
                },
                new ExecutionDataflowBlockOptions { CancellationToken = cancelToken, MaxDegreeOfParallelism = 16 });
        }

        private static ActionBlock<Tuple<LoadedTx, int>> InitScriptValidator(IBlockchainRules rules, ChainedHeader chainedHeader, CancellationToken cancelToken)
        {
            return new ActionBlock<Tuple<LoadedTx, int>>(
                tuple =>
                {
                    var loadedTx = tuple.Item1;
                    var inputIndex = tuple.Item2;
                    var txInput = loadedTx.Transaction.Inputs[inputIndex];
                    var prevTxOutput = loadedTx.GetInputPrevTxOutput(inputIndex);

                    if (!rules.IgnoreScriptErrors)
                    {
                        rules.ValidationTransactionScript(chainedHeader, loadedTx.Transaction, loadedTx.TxIndex, txInput, inputIndex, prevTxOutput);
                    }
                    else
                    {
                        try
                        {
                            rules.ValidationTransactionScript(chainedHeader, loadedTx.Transaction, loadedTx.TxIndex, txInput, inputIndex, prevTxOutput);
                        }
                        catch (Exception ex)
                        {
                            var aggEx = ex as AggregateException;
                            logger.Debug("Ignoring script errors in block: {0,9:#,##0}, errors: {1:#,##0}".Format2(chainedHeader.Height, aggEx != null ? aggEx.InnerExceptions.Count : -1));
                        }
                    }
                },
                new ExecutionDataflowBlockOptions { CancellationToken = cancelToken, MaxDegreeOfParallelism = 16 });
        }
    }
}
