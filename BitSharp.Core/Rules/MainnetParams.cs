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
    public partial class MainnetParams : IChainParams
    {
        private const UInt64 SATOSHI_PER_BTC = 100 * 1000 * 1000;

        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly ChainedHeader genesisChainedHeader;
        private readonly int difficultyInterval = 2016;
        private readonly long difficultyTargetTimespan = 14 * 24 * 60 * 60;

        public MainnetParams()
        {
            this.genesisChainedHeader = ChainedHeader.CreateForGenesisBlock(this.genesisBlock.Header);
        }

        //TODO should be 00000000ffffffffffffffffffffffffffffffffffffffffffffffffffffffff
        public virtual UInt256 HighestTarget { get; } = UInt256.ParseHex("00000000FFFF0000000000000000000000000000000000000000000000000000");

        public virtual Block GenesisBlock => this.genesisBlock;

        public virtual ChainedHeader GenesisChainedHeader => this.genesisChainedHeader;

        public virtual int DifficultyInterval => this.difficultyInterval;

        public virtual long DifficultyTargetTimespan => this.difficultyTargetTimespan;

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

        public virtual void PreValidateBlock(Chain chain, ChainedHeader chainedHeader)
        {
            // calculate the next required target
            var requiredTarget = GetRequiredNextTarget(chain);
            if (requiredTarget > HighestTarget)
                requiredTarget = HighestTarget;

            // validate block's target against the required target
            var blockTarget = chainedHeader.BlockHeader.CalculateTarget();
            if (blockTarget != requiredTarget)
            {
                throw new ValidationException(chainedHeader.Hash,
                    $"Failing block {chainedHeader.Hash} at height {chainedHeader.Height}: Block target {blockTarget} did not match required target of {requiredTarget}");
            }

            // validate block's proof of work against its stated target
            if (chainedHeader.Hash > blockTarget || chainedHeader.Hash > requiredTarget)
            {
                throw new ValidationException(chainedHeader.Hash,
                    $"Failing block {chainedHeader.Hash} at height {chainedHeader.Height}: Block did not match its own target of {blockTarget}");
            }

            //TODO
            // calculate adjusted time
            var adjustedTime = DateTimeOffset.UtcNow;

            // calculate max block time
            var maxBlockTime = adjustedTime + TimeSpan.FromHours(2);

            // verify max block time
            var blockTime = DateTimeOffset.FromUnixTimeSeconds(chainedHeader.Time);
            if (blockTime > maxBlockTime)
                throw new ValidationException(chainedHeader.Hash);

            // ensure timestamp is greater than the median timestamp of the previous 11 blocks
            if (chainedHeader.Height > 0)
            {
                var medianTimeSpan = Math.Min(11, chain.Blocks.Count - 1);

                var prevHeaderTimes = chain.Blocks.GetRange(chain.Blocks.Count - 1 - medianTimeSpan, medianTimeSpan)
                    //TODO pull tester doesn't fail if the sort step is missed
                    .OrderBy(x => x.Time).ToList();

                var medianPrevHeaderTime = DateTimeOffset.FromUnixTimeSeconds(prevHeaderTimes[prevHeaderTimes.Count / 2].Time);

                if (blockTime <= medianPrevHeaderTime)
                {
                    throw new ValidationException(chainedHeader.Hash,
                        $"Failing block {chainedHeader.Hash} at height {chainedHeader.Height}: Block's time of {blockTime} must be greater than past median time of {medianPrevHeaderTime}");
                }
            }

            //TODO: ContextualCheckBlockHeader nVersion checks
        }

        public virtual void PostValidateBlock(Chain chain, ChainedHeader chainedHeader, Transaction coinbaseTx, ulong totalTxInputValue, ulong totalTxOutputValue)
        {
            // ensure there is at least 1 transaction
            if (coinbaseTx == null)
            {
                throw new ValidationException(chainedHeader.Hash,
                    $"Failing block {chainedHeader.Hash} at height {chainedHeader.Height}: Zero transactions present");
            }

            // check that coinbase has only one input
            if (coinbaseTx.Inputs.Length != 1)
            {
                throw new ValidationException(chainedHeader.Hash,
                    $"Failing block {chainedHeader.Hash} at height {chainedHeader.Height}: Coinbase transaction does not have exactly one input");
            }

            // validate transactions
            var blockUnspentValue = totalTxInputValue - totalTxOutputValue;

            // calculate the expected reward in coinbase
            var expectedReward = (ulong)(50 * SATOSHI_PER_BTC);
            if (chainedHeader.Height / 210000 <= 32)
                expectedReward /= (ulong)Math.Pow(2, chainedHeader.Height / 210000);
            expectedReward += blockUnspentValue;

            // calculate the actual reward in coinbase
            var actualReward = coinbaseTx.Outputs.Sum(x => x.Value);

            // ensure coinbase has correct reward
            if (actualReward > expectedReward)
            {
                throw new ValidationException(chainedHeader.Hash,
                    $"Failing block {chainedHeader.Hash} at height {chainedHeader.Height}: Coinbase value is greater than reward + fees");
            }
        }

        public virtual void ValidateTransaction(ChainedHeader chainedHeader, ValidatableTx validatableTx)
        {
            var tx = validatableTx.Transaction;
            var txIndex = validatableTx.Index;

            if (validatableTx.IsCoinbase)
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

                    //TODO
                    var COINBASE_MATURITY = 100;
                    if (prevOutput.IsCoinbase
                        && chainedHeader.Height - prevOutput.BlockHeight < COINBASE_MATURITY)
                    {
                        throw new ValidationException(chainedHeader.Hash);
                    }

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

        public virtual void ValidationTransactionScript(ChainedHeader chainedHeader, BlockTx tx, TxInput txInput, int txInputIndex, PrevTxOutput prevTxOutput)
        {
            // BIP16 didn't become active until Apr 1 2012
            var nBIP16SwitchTime = 1333238400U;
            var strictPayToScriptHash = chainedHeader.Time >= nBIP16SwitchTime;

            var flags = strictPayToScriptHash ? verify_flags_type.verify_flags_p2sh : verify_flags_type.verify_flags_none;

            // Start enforcing the DERSIG (BIP66) rules, for block.nVersion=3 blocks,
            // when 75% of the network has upgraded:
            if (chainedHeader.Version >= 3)
            //&& IsSuperMajority(3, pindex->pprev, chainparams.GetConsensus().nMajorityEnforceBlockUpgrade, chainparams.GetConsensus())
            {
                flags |= verify_flags_type.verify_flags_dersig;
            }

            // Start enforcing CHECKLOCKTIMEVERIFY, (BIP65) for block.nVersion=4
            // blocks, when 75% of the network has upgraded:
            if (chainedHeader.Version >= 4)
            //&& IsSuperMajority(4, pindex->pprev, chainparams.GetConsensus().nMajorityEnforceBlockUpgrade, chainparams.GetConsensus()))
            {
                flags |= verify_flags_type.verify_flags_checklocktimeverify;
            }

            var result = LibbitcoinConsensus.VerifyScript(
                tx.TxBytes,
                prevTxOutput.ScriptPublicKey,
                txInputIndex,
                flags);

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
