using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Script;
using BitSharp.Core.Storage;
using NLog;
using System;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BitSharp.Core.Builders
{
    internal static class BlockValidator
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        public static async Task ValidateBlockAsync(ICoreStorage coreStorage, IBlockchainRules rules, Chain chain, ChainedHeader chainedHeader, ISourceBlock<ValidatableTx> validatableTxes, CancellationToken cancelToken = default(CancellationToken))
        {
            // pre-validate block before doing any more work
            rules.PreValidateBlock(chain, chainedHeader);

            // validate merkle root
            var merkleStream = new MerkleStream<BlockTxNode>();
            var merkleValidator = InitMerkleValidator(chainedHeader, merkleStream, cancelToken);

            // begin feeding the merkle validator
            validatableTxes.LinkTo(merkleValidator, new DataflowLinkOptions { PropagateCompletion = true });

            // capture fees
            Transaction coinbaseTx = null;
            var totalTxInputValue = 0UL;
            var totalTxOutputValue = 0UL;
            var totalSigOpCount = 0;
            var feeCapturer = new TransformBlock<ValidatableTx, ValidatableTx>(
                validatableTx =>
                {
                    if (validatableTx.IsCoinbase)
                    {
                        coinbaseTx = validatableTx.Transaction;
                    }
                    else
                    {
                        totalTxInputValue += validatableTx.PrevTxOutputs.Sum(x => x.Value);
                        totalTxOutputValue += validatableTx.Transaction.Outputs.Sum(x => x.Value);
                    }

                    totalSigOpCount += validatableTx.Transaction.Inputs.Sum(x => CountSigOps(x.ScriptSignature));
                    totalSigOpCount += validatableTx.Transaction.Outputs.Sum(x => CountSigOps(x.ScriptPublicKey));

                    //TODO
                    var MAX_BLOCK_SIGOPS = 20.THOUSAND();
                    if (totalSigOpCount > MAX_BLOCK_SIGOPS)
                        throw new ValidationException(chainedHeader.Hash);

                    return validatableTx;
                });
            merkleValidator.LinkTo(feeCapturer, new DataflowLinkOptions { PropagateCompletion = true });

            // validate transactions
            var txValidator = InitTxValidator(rules, chainedHeader, cancelToken);

            // begin feeding the tx validator
            feeCapturer.LinkTo(txValidator, new DataflowLinkOptions { PropagateCompletion = true });

            // validate scripts
            var scriptValidator = InitScriptValidator(rules, chainedHeader, cancelToken);

            // begin feeding the script validator
            txValidator.LinkTo(scriptValidator, new DataflowLinkOptions { PropagateCompletion = true });

            await merkleValidator.Completion;
            await feeCapturer.Completion;
            await txValidator.Completion;
            await scriptValidator.Completion;

            // validate overall block
            rules.PostValidateBlock(chain, chainedHeader, coinbaseTx, totalTxInputValue, totalTxOutputValue);

            if (!rules.BypassPrevTxLoading)
            {
                try
                {
                    merkleStream.FinishPairing();
                }
                //TODO
                catch (InvalidOperationException)
                {
                    throw CreateMerkleRootException(chainedHeader);
                }
                if (merkleStream.RootNode.Hash != chainedHeader.MerkleRoot)
                    throw CreateMerkleRootException(chainedHeader);
            }
        }

        private static TransformBlock<ValidatableTx, ValidatableTx> InitMerkleValidator(ChainedHeader chainedHeader, MerkleStream<BlockTxNode> merkleStream, CancellationToken cancelToken)
        {
            return new TransformBlock<ValidatableTx, ValidatableTx>(
                validatableTx =>
                {
                    try
                    {
                        merkleStream.AddNode(validatableTx.BlockTx);
                    }
                    //TODO
                    catch (InvalidOperationException)
                    {
                        throw CreateMerkleRootException(chainedHeader);
                    }
                    return validatableTx;
                },
                new ExecutionDataflowBlockOptions { CancellationToken = cancelToken });
        }

        private static TransformManyBlock<ValidatableTx, Tuple<ValidatableTx, int>> InitTxValidator(IBlockchainRules rules, ChainedHeader chainedHeader, CancellationToken cancelToken)
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

        private static ActionBlock<Tuple<ValidatableTx, int>> InitScriptValidator(IBlockchainRules rules, ChainedHeader chainedHeader, CancellationToken cancelToken)
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

        private static ValidationException CreateMerkleRootException(ChainedHeader chainedHeader)
        {
            return new ValidationException(chainedHeader.Hash, $"Failing block {chainedHeader.Hash} at height {chainedHeader.Height}: Merkle root is invalid");
        }

        //TODO - hasn't been checked for correctness, should also be moved
        private static int CountSigOps(ImmutableArray<byte> script)
        {
            var sigOpCount = 0;

            var index = 0;
            while (index < script.Length)
            {
                var opByte = script[index++];
                var op = (ScriptOp)Enum.ToObject(typeof(ScriptOp), opByte);

                switch (op)
                {
                    case ScriptOp.OP_CHECKSIG:
                    case ScriptOp.OP_CHECKSIGVERIFY:
                        sigOpCount++;
                        break;

                    case ScriptOp.OP_CHECKMULTISIG:
                    case ScriptOp.OP_CHECKMULTISIGVERIFY:
                        //TODO
                        var MAX_PUBKEYS_PER_MULTISIG = 20;
                        var prevOpCode = index >= 2 ? script[index - 2] : (byte)ScriptOp.OP_INVALIDOPCODE;
                        if (prevOpCode >= (byte)ScriptOp.OP_1 && prevOpCode <= (byte)ScriptOp.OP_16)
                            sigOpCount += prevOpCode;
                        else
                            sigOpCount += MAX_PUBKEYS_PER_MULTISIG;

                        break;
                }

                if (op <= ScriptOp.OP_PUSHDATA4)
                {
                    //OP_PUSHBYTES1-75
                    uint dataLength;
                    if (op < ScriptOp.OP_PUSHDATA1)
                    {
                        dataLength = opByte;
                    }
                    else if (op == ScriptOp.OP_PUSHDATA1)
                    {
                        if (index + 1 > script.Length)
                            break;

                        dataLength = script[index++];
                    }
                    else if (op == ScriptOp.OP_PUSHDATA2)
                    {
                        if (index + 2 > script.Length)
                            break;

                        dataLength = (uint)script[index++] + ((uint)script[index++] << 8);
                    }
                    else if (op == ScriptOp.OP_PUSHDATA4)
                    {
                        if (index + 4 > script.Length)
                            break;

                        dataLength = (uint)script[index++] + ((uint)script[index++] << 8) + ((uint)script[index++] << 16) + ((uint)script[index++] << 24);
                    }
                    else
                    {
                        dataLength = 0;
                        Debug.Assert(false);
                    }

                    if ((ulong)index + dataLength >= (uint)script.Length)
                        break;
                    else
                        index += (int)dataLength;
                }
            }

            return sigOpCount;
        }
    }
}
