using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Script;
using NLog;
using System;
using System.Collections.Immutable;
using System.Linq;
using System.Numerics;

namespace BitSharp.Core.Rules
{
    public class MainnetRules : IBlockchainRules
    {
        private const UInt64 SATOSHI_PER_BTC = 100 * 1000 * 1000;

        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly UInt256 highestTarget;
        private readonly Block genesisBlock;
        private readonly ChainedHeader genesisChainedHeader;
        private readonly int difficultyInterval = 2016;
        private readonly long difficultyTargetTimespan = 14 * 24 * 60 * 60;

        public MainnetRules()
        {
            this.highestTarget = UInt256.ParseHex("00000000FFFF0000000000000000000000000000000000000000000000000000");

            this.genesisBlock =
                new Block
                (
                    header: new BlockHeader
                    (
                        version: 1,
                        previousBlock: UInt256.Zero,
                        merkleRoot: UInt256.ParseHex("4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"),
                        time: 1231006505,
                        bits: 0x1D00FFFF,
                        nonce: 2083236893
                    ),
                    transactions: ImmutableArray.Create
                    (
                        new Transaction
                        (
                            version: 1,
                            inputs: ImmutableArray.Create
                            (
                                new TxInput
                                (
                                    previousTxOutputKey: new TxOutputKey
                                    (
                                        txHash: UInt256.Zero,
                                        txOutputIndex: 0xFFFFFFFF
                                    ),
                                    scriptSignature: ImmutableArray.Create<byte>
                                    (
                                        0x04, 0xFF, 0xFF, 0x00, 0x1D, 0x01, 0x04, 0x45, 0x54, 0x68, 0x65, 0x20, 0x54, 0x69, 0x6D, 0x65,
                                        0x73, 0x20, 0x30, 0x33, 0x2F, 0x4A, 0x61, 0x6E, 0x2F, 0x32, 0x30, 0x30, 0x39, 0x20, 0x43, 0x68,
                                        0x61, 0x6E, 0x63, 0x65, 0x6C, 0x6C, 0x6F, 0x72, 0x20, 0x6F, 0x6E, 0x20, 0x62, 0x72, 0x69, 0x6E,
                                        0x6B, 0x20, 0x6F, 0x66, 0x20, 0x73, 0x65, 0x63, 0x6F, 0x6E, 0x64, 0x20, 0x62, 0x61, 0x69, 0x6C,
                                        0x6F, 0x75, 0x74, 0x20, 0x66, 0x6F, 0x72, 0x20, 0x62, 0x61, 0x6E, 0x6B, 0x73
                                    ),
                                    sequence: 0xFFFFFFFF
                                )
                            ),
                            outputs: ImmutableArray.Create
                            (
                                new TxOutput
                                (
                                    value: 50 * SATOSHI_PER_BTC,
                                    scriptPublicKey: ImmutableArray.Create<byte>
                                    (
                                        0x41, 0x04, 0x67, 0x8A, 0xFD, 0xB0, 0xFE, 0x55, 0x48, 0x27, 0x19, 0x67, 0xF1, 0xA6, 0x71, 0x30,
                                        0xB7, 0x10, 0x5C, 0xD6, 0xA8, 0x28, 0xE0, 0x39, 0x09, 0xA6, 0x79, 0x62, 0xE0, 0xEA, 0x1F, 0x61,
                                        0xDE, 0xB6, 0x49, 0xF6, 0xBC, 0x3F, 0x4C, 0xEF, 0x38, 0xC4, 0xF3, 0x55, 0x04, 0xE5, 0x1E, 0xC1,
                                        0x12, 0xDE, 0x5C, 0x38, 0x4D, 0xF7, 0xBA, 0x0B, 0x8D, 0x57, 0x8A, 0x4C, 0x70, 0x2B, 0x6B, 0xF1,
                                        0x1D, 0x5F, 0xAC
                                    )
                                )
                            ),
                            lockTime: 0
                        )
                    )
                );

            this.genesisChainedHeader = ChainedHeader.CreateForGenesisBlock(this.genesisBlock.Header);
        }

        //TODO
        public bool BypassPrevTxLoading { get; set; }
        public bool IgnoreScripts { get; set; }
        public bool IgnoreSignatures { get; set; }
        public bool IgnoreScriptErrors { get; set; }

        public virtual UInt256 HighestTarget => this.highestTarget;

        public virtual Block GenesisBlock => this.genesisBlock;

        public virtual ChainedHeader GenesisChainedHeader => this.genesisChainedHeader;

        public virtual int DifficultyInterval => this.difficultyInterval;

        public virtual long DifficultyTargetTimespan => this.difficultyTargetTimespan;

        public virtual double TargetToDifficulty(UInt256 target)
        {
            // difficulty is HighestTarget / target
            // since these are 256-bit numbers, use division trick for BigIntegers
            return Math.Exp(BigInteger.Log(HighestTarget.ToBigInteger()) - BigInteger.Log(target.ToBigInteger()));
        }

        public virtual UInt256 DifficultyToTarget(double difficulty)
        {
            // implementation is equivalent of HighestTarget / difficulty

            // multiply difficulty and HighestTarget by a scale so that the decimal portion can be fed into a BigInteger
            var scale = 0x100000000L;
            var highestTargetScaled = (BigInteger)HighestTarget * scale;
            var difficultyScaled = (BigInteger)(difficulty * scale);

            // do the division
            var target = highestTargetScaled / difficultyScaled;

            // get the resulting target bytes, taking only the 3 most significant
            var targetBytes = target.ToByteArray();
            targetBytes = new byte[targetBytes.Length - 3].Concat(targetBytes.Skip(targetBytes.Length - 3).ToArray());

            // return the target
            return new UInt256(targetBytes);
        }

        public virtual UInt256 GetRequiredNextTarget(Chain chain)
        {
            try
            {
                // genesis block, use its target
                if (chain.Height == 0)
                {
                    // lookup genesis block header
                    var genesisBlockHeader = chain.Blocks[0].BlockHeader;

                    return genesisBlockHeader.CalculateTarget();
                }
                // not on an adjustment interval, use previous block's target
                else if (chain.Height % DifficultyInterval != 0)
                {
                    // lookup the previous block on the current blockchain
                    var prevBlockHeader = chain.Blocks[chain.Height - 1].BlockHeader;

                    return prevBlockHeader.CalculateTarget();
                }
                // on an adjustment interval, calculate the required next target
                else
                {
                    // lookup the previous block on the current blockchain
                    var prevBlockHeader = chain.Blocks[chain.Height - 1].BlockHeader;

                    // get the block difficultyInterval blocks ago
                    var startBlockHeader = chain.Blocks[chain.Height - DifficultyInterval].BlockHeader;
                    //Debug.Assert(startChainedHeader.Height == blockchain.Height - DifficultyInternal);

                    var actualTimespan = (long)prevBlockHeader.Time - (long)startBlockHeader.Time;
                    var targetTimespan = DifficultyTargetTimespan;

                    // limit adjustment to 4x or 1/4x
                    if (actualTimespan < targetTimespan / 4)
                        actualTimespan = targetTimespan / 4;
                    else if (actualTimespan > targetTimespan * 4)
                        actualTimespan = targetTimespan * 4;

                    // calculate the new target
                    var target = startBlockHeader.CalculateTarget();
                    target *= (UInt256)actualTimespan;
                    target /= (UInt256)targetTimespan;

                    // make sure target isn't too high (too low difficulty)
                    if (target > HighestTarget)
                        target = HighestTarget;

                    return target;
                }
            }
            catch (ArgumentException)
            {
                // invalid bits
                throw new ValidationException(chain.LastBlock.Hash);
            }
        }

        //TODO
        //public virtual void ValidateBlock(ChainedBlock chainedBlock, ChainStateBuilder chainStateBuilder)
        //{
        //    //TODO
        //    if (BypassValidation)
        //        return;

        //    // calculate the next required target
        //    var requiredTarget = GetRequiredNextTarget(chainStateBuilder.Chain);

        //    // validate block's target against the required target
        //    var blockTarget = chainedBlock.Header.CalculateTarget();
        //    if (blockTarget > requiredTarget)
        //    {
        //        throw new ValidationException(chainedBlock.Hash, "Failing block {0} at height {1}: Block target {2} did not match required target of {3}".Format2(chainedBlock.Hash.ToHexNumberString(), chainedBlock.Height, blockTarget.ToHexNumberString(), requiredTarget.ToHexNumberString()));
        //    }

        //    // validate block's proof of work against its stated target
        //    if (chainedBlock.Hash > blockTarget || chainedBlock.Hash > requiredTarget)
        //    {
        //        throw new ValidationException(chainedBlock.Hash, "Failing block {0} at height {1}: Block did not match its own target of {2}".Format2(chainedBlock.Hash.ToHexNumberString(), chainedBlock.Height, blockTarget.ToHexNumberString()));
        //    }

        //    // ensure there is at least 1 transaction
        //    if (chainedBlock.Transactions.Length == 0)
        //    {
        //        throw new ValidationException(chainedBlock.Hash, "Failing block {0} at height {1}: Zero transactions present".Format2(chainedBlock.Hash.ToHexNumberString(), chainedBlock.Height));
        //    }

        //    //TODO apply real coinbase rule
        //    // https://github.com/bitcoin/bitcoin/blob/481d89979457d69da07edd99fba451fd42a47f5c/src/core.h#L219
        //    var coinbaseTx = chainedBlock.Transactions[0];

        //    // check that coinbase has only one input
        //    if (coinbaseTx.Inputs.Length != 1)
        //    {
        //        throw new ValidationException(chainedBlock.Hash, "Failing block {0} at height {1}: Coinbase transaction does not have exactly one input".Format2(chainedBlock.Hash.ToHexNumberString(), chainedBlock.Height));
        //    }

        //    var blockTxIndices = new Dictionary<UInt256, int>();
        //    for (var i = 0; i < chainedBlock.Transactions.Length; i++)
        //        blockTxIndices.Add(chainedBlock.Transactions[i].Hash, i);

        //    // validate transactions
        //    long blockUnspentValue = 0L;
        //    for (var txIndex = 1; txIndex < chainedBlock.Transactions.Length; txIndex++)
        //    {
        //        var tx = chainedBlock.Transactions[txIndex];

        //        long txUnspentValue;
        //        ValidateTransaction(chainedBlock, tx, txIndex, chainStateBuilder, out txUnspentValue, blockTxIndices);

        //        blockUnspentValue += txUnspentValue;
        //    }

        //    // calculate the expected reward in coinbase
        //    var expectedReward = (long)(50 * SATOSHI_PER_BTC);
        //    if (chainedBlock.Height / 210000 <= 32)
        //        expectedReward /= (long)Math.Pow(2, chainedBlock.Height / 210000);
        //    expectedReward += blockUnspentValue;

        //    // calculate the actual reward in coinbase
        //    var actualReward = 0L;
        //    foreach (var txOutput in coinbaseTx.Outputs)
        //        actualReward += (long)txOutput.Value;

        //    // ensure coinbase has correct reward
        //    if (actualReward > expectedReward)
        //    {
        //        throw new ValidationException(chainedBlock.Hash, "Failing block {0} at height {1}: Coinbase value is greater than reward + fees".Format2(chainedBlock.Hash.ToHexNumberString(), chainedBlock.Height));
        //    }

        //    // all validation has passed
        //}

        public virtual void ValidateTransaction(ChainedHeader chainedHeader, ValidatableTx validatableTx)
        {
            var tx = validatableTx.Transaction.Decode();
            var txIndex = validatableTx.Transaction.Index;

            if (validatableTx.Transaction.IsCoinbase)
            {
                // TODO coinbase tx validation
            }
            else
            {
                // verify spend amounts
                var txInputValue = (UInt64)0;
                var txOutputValue = (UInt64)0;

                for (var inputIndex = 0; inputIndex < tx.Inputs.Length; inputIndex++)
                {
                    var input = tx.Inputs[inputIndex];
                    var prevOutput = validatableTx.PrevTxOutputs[inputIndex];

                    // add transactions previous value to unspent amount (used to calculate allowed coinbase reward)
                    txInputValue += prevOutput.Value;
                }

                for (var outputIndex = 0; outputIndex < tx.Outputs.Length; outputIndex++)
                {
                    // remove transactions spend value from unspent amount (used to calculate allowed coinbase reward)
                    var output = tx.Outputs[outputIndex];
                    txOutputValue += output.Value;
                }

                // ensure that amount being output from transaction isn't greater than amount being input
                if (txOutputValue > txInputValue)
                {
                    throw new ValidationException(chainedHeader.Hash, $"Failing tx {tx.Hash}: Transaction output value is greater than input value");
                }

                // calculate fee value (unspent amount)
                var feeValue = (long)(txInputValue - txOutputValue);

                // sanity check
                if (feeValue < 0)
                {
                    throw new ValidationException(chainedHeader.Hash);
                }
            }

            // all validation has passed
        }

        public virtual void ValidationTransactionScript(ChainedHeader chainedHeader, BlockTx tx, TxInput txInput, int txInputIndex, TxOutput prevTxOutput)
        {
            var result = LibConsensus.VerifyScript(
                tx.TxBytes.Value,
                prevTxOutput.ScriptPublicKey,
                txInputIndex);

            //var scriptEngine = new ScriptEngine(this.IgnoreSignatures);

            //// create the transaction script from the input and output
            //var script = txInput.ScriptSignature.Concat(prevTxOutput.ScriptPublicKey);
            //if (!scriptEngine.VerifyScript(chainedHeader.Hash, txIndex, prevTxOutput.ScriptPublicKey.ToArray(), tx, txInputIndex, script.ToArray()))
            if (!result)
            {
                logger.Debug($"Script did not pass in block: {chainedHeader.Hash}, tx: {tx.Index}, {tx.Hash}, input: {txInputIndex}");
                throw new ValidationException(chainedHeader.Hash);
            }
        }
    }
}
