using BitSharp.Common;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using NLog;
using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BitSharp.Core.Builders
{
    internal static class BlockValidator
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        public static async Task ValidateBlockAsync(ICoreStorage coreStorage, ICoreRules rules, Chain chain, ChainedHeader chainedHeader, ISourceBlock<ValidatableTx> validatableTxes, CancellationToken cancelToken = default(CancellationToken))
        {
            // tally transactions
            object finalTally = null;
            var txTallier = new TransformBlock<ValidatableTx, ValidatableTx>(
                validatableTx =>
                {
                    var runningTally = finalTally;
                    rules.TallyTransaction(chainedHeader, validatableTx, ref runningTally);
                    finalTally = runningTally;

                    return validatableTx;
                });
            validatableTxes.LinkTo(txTallier, new DataflowLinkOptions { PropagateCompletion = true });

            // validate transactions
            var txValidator = InitTxValidator(rules, chainedHeader, cancelToken);

            // begin feeding the tx validator
            txTallier.LinkTo(txValidator, new DataflowLinkOptions { PropagateCompletion = true });

            // validate scripts
            var scriptValidator = InitScriptValidator(rules, chainedHeader, cancelToken);

            // begin feeding the script validator
            txValidator.LinkTo(scriptValidator, new DataflowLinkOptions { PropagateCompletion = true });

            //TODO
            await PipelineCompletion.Create(
                new Task[] { },
                new IDataflowBlock[] { validatableTxes, txTallier, txValidator, scriptValidator });

            // validate overall block
            rules.PostValidateBlock(chain, chainedHeader, finalTally);
        }

        private static TransformManyBlock<ValidatableTx, Tuple<ValidatableTx, int>> InitTxValidator(ICoreRules rules, ChainedHeader chainedHeader, CancellationToken cancelToken)
        {
            return new TransformManyBlock<ValidatableTx, Tuple<ValidatableTx, int>>(
                validatableTx =>
                {
                    rules.ValidateTransaction(chainedHeader, validatableTx);

                    if (!rules.IgnoreScripts && !validatableTx.IsCoinbase)
                    {
                        var tx = validatableTx.Transaction;

                        var scripts = new Tuple<ValidatableTx, int>[tx.Inputs.Length];
                        for (var i = 0; i < tx.Inputs.Length; i++)
                            scripts[i] = Tuple.Create(validatableTx, i);

                        return scripts;
                    }
                    else
                        return new Tuple<ValidatableTx, int>[0];
                },
                new ExecutionDataflowBlockOptions { CancellationToken = cancelToken, MaxDegreeOfParallelism = Environment.ProcessorCount });
        }

        private static ActionBlock<Tuple<ValidatableTx, int>> InitScriptValidator(ICoreRules rules, ChainedHeader chainedHeader, CancellationToken cancelToken)
        {
            return new ActionBlock<Tuple<ValidatableTx, int>>(
                tuple =>
                {
                    var validatableTx = tuple.Item1;
                    var inputIndex = tuple.Item2;
                    var tx = validatableTx.Transaction;
                    var txInput = tx.Inputs[inputIndex];
                    var prevTxOutputs = validatableTx.PrevTxOutputs[inputIndex];

                    if (!rules.IgnoreScriptErrors)
                    {
                        rules.ValidationTransactionScript(chainedHeader, validatableTx.BlockTx, txInput, inputIndex, prevTxOutputs);
                    }
                    else
                    {
                        try
                        {
                            rules.ValidationTransactionScript(chainedHeader, validatableTx.BlockTx, txInput, inputIndex, prevTxOutputs);
                        }
                        catch (Exception ex)
                        {
                            var aggEx = ex as AggregateException;
                            logger.Debug($"Ignoring script errors in block: {chainedHeader.Height,9:N0}, errors: {(aggEx?.InnerExceptions.Count ?? -1):N0}");
                        }
                    }
                },
                new ExecutionDataflowBlockOptions { CancellationToken = cancelToken, MaxDegreeOfParallelism = Environment.ProcessorCount });
        }
    }
}
