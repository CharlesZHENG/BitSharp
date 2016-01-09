using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Script;
using NLog;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Numerics;

namespace BitSharp.Core.Rules
{
    public class CoreRules : ICoreRules
    {
        //TODO
        public bool IgnoreScripts { get; set; }
        public bool IgnoreSignatures { get; set; }
        public bool IgnoreScriptErrors { get; set; }

        private const UInt64 SATOSHI_PER_BTC = 100 * 1000 * 1000;
        private static readonly ulong MAX_MONEY = (ulong)21.MILLION() * (ulong)100.MILLION();
        private const int COINBASE_MATURITY = 100;

        private static readonly int MAX_BLOCK_SIZE = 1.MILLION(); // :(
        private static readonly int MAX_BLOCK_SIGOPS = 20.THOUSAND();
        private const int MAX_PUBKEYS_PER_MULTISIG = 20;

        private const int LOCKTIME_MEDIAN_TIME_PAST = 1 << 1;
        // Threshold for nLockTime: below this value it is interpreted as block number,
        // otherwise as UNIX timestamp.
        private const uint LOCKTIME_THRESHOLD = 500000000; // Tue Nov  5 00:53:20 1985 UTC

        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        public CoreRules(IChainParams chainParams)
        {
            ChainParams = chainParams;
        }

        public IChainParams ChainParams { get; }

        public void PreValidateBlock(Chain newChain)
        {
            var chainedHeader = newChain.LastBlock;

            // calculate the next required target
            var requiredBits = GetRequiredNextBits(newChain);

            // validate required target
            var blockTarget = chainedHeader.BlockHeader.CalculateTarget();
            if (blockTarget > ChainParams.HighestTarget)
                throw new ValidationException(chainedHeader.Hash);

            // validate block's target against the required target
            if (chainedHeader.Bits != requiredBits)
            {
                throw new ValidationException(chainedHeader.Hash,
                    $"Failing block {chainedHeader.Hash} at height {chainedHeader.Height}: Block bits {chainedHeader.Bits} did not match required bits of {requiredBits}");
            }

            // validate block's proof of work against its stated target
            if (chainedHeader.Hash > blockTarget)
            {
                throw new ValidationException(chainedHeader.Hash,
                    $"Failing block {chainedHeader.Hash} at height {chainedHeader.Height}: Block did not match its own target of {blockTarget}");
            }

            //TODO
            // calculate adjusted time
            var adjustedTime = DateTimeOffset.Now;

            // calculate max block time
            var maxBlockTime = adjustedTime + TimeSpan.FromHours(2);

            // verify max block time
            var blockTime = chainedHeader.Time;
            if (blockTime > maxBlockTime)
                throw new ValidationException(chainedHeader.Hash);

            // ensure timestamp is greater than the median timestamp of the previous 11 blocks
            if (chainedHeader.Height > 0)
            {
                var medianTimeSpan = Math.Min(11, newChain.Blocks.Count - 1);

                var prevHeaderTimes = newChain.Blocks.GetRange(newChain.Blocks.Count - 1 - medianTimeSpan, medianTimeSpan)
                    //TODO pull tester doesn't fail if the sort step is missed
                    .OrderBy(x => x.Time).ToList();

                var medianPrevHeaderTime = GetMedianPrevHeaderTime(newChain, chainedHeader.Height);

                if (blockTime <= medianPrevHeaderTime)
                {
                    throw new ValidationException(chainedHeader.Hash,
                        $"Failing block {chainedHeader.Hash} at height {chainedHeader.Height}: Block's time of {blockTime} must be greater than past median time of {medianPrevHeaderTime}");
                }
            }

            //TODO: ContextualCheckBlockHeader nVersion checks
        }

        public void TallyTransaction(Chain newChain, ValidatableTx validatableTx, ref object runningTally)
        {
            var chainedHeader = newChain.LastBlock;

            if (runningTally == null)
            {
                var medianPrevHeaderTime = GetMedianPrevHeaderTime(newChain, chainedHeader.Height);

                var lockTimeFlags = 0; // TODO why is this used here?
                var lockTimeCutoff = ((lockTimeFlags & LOCKTIME_MEDIAN_TIME_PAST) != 0)
                                      ? GetMedianPrevHeaderTime(newChain, chainedHeader.Height).ToUnixTimeSeconds()
                                      : chainedHeader.Time.ToUnixTimeSeconds();

                runningTally = new BlockTally { BlockSize = 80, LockTimeCutoff = lockTimeCutoff };
            }

            var blockTally = (BlockTally)runningTally;

            var tx = validatableTx.Transaction;
            var txIndex = validatableTx.Index;

            // BIP16 didn't become active until Apr 1 2012
            var nBIP16SwitchTime = DateTimeOffset.FromUnixTimeSeconds(1333238400U);
            var strictPayToScriptHash = chainedHeader.Time >= nBIP16SwitchTime;

            // first transaction must be coinbase
            if (validatableTx.Index == 0 && !tx.IsCoinbase)
                throw new ValidationException(chainedHeader.Hash);
            // all other transactions must not be coinbase
            else if (validatableTx.Index > 0 && tx.IsCoinbase)
                throw new ValidationException(chainedHeader.Hash);
            // must have inputs
            else if (tx.Inputs.Length == 0)
                throw new ValidationException(chainedHeader.Hash);
            // must have outputs
            else if (tx.Outputs.Length == 0)
                throw new ValidationException(chainedHeader.Hash);
            // coinbase scriptSignature length must be >= 2 && <= 100
            else if (tx.IsCoinbase && (tx.Inputs[0].ScriptSignature.Length < 2 || tx.Inputs[0].ScriptSignature.Length > 100))
                throw new ValidationException(chainedHeader.Hash);
            // all transactions must be finalized
            else if (!IsFinal(tx, chainedHeader.Height, blockTally.LockTimeCutoff))
                throw new ValidationException(chainedHeader.Hash);

            // Enforce block.nVersion=2 rule that the coinbase starts with serialized block height
            // if 750 of the last 1,000 blocks are version 2 or greater (51/100 if testnet):
            if (tx.IsCoinbase
                && chainedHeader.Version >= 2
                && IsSuperMajority(2, newChain, ChainParams.MajorityEnforceBlockUpgrade))
            {
                var requiredScript = GetPushInt64Script(chainedHeader.Height);
                var actualScript = tx.Inputs[0].ScriptSignature;

                if (actualScript.Length < requiredScript.Length
                    || !actualScript.Take(requiredScript.Length).SequenceEqual(requiredScript))
                {
                    throw new ValidationException(chainedHeader.Hash);
                }
            }

            // running input/output value tallies
            var txTotalInputValue = 0UL;
            var txTotalOutputValue = 0UL;
            var txSigOpCount = 0;

            // validate & tally inputs
            for (var inputIndex = 0; inputIndex < tx.Inputs.Length; inputIndex++)
            {
                var input = tx.Inputs[inputIndex];

                if (!tx.IsCoinbase)
                {
                    var prevOutput = validatableTx.PrevTxOutputs[inputIndex];

                    // if spending a coinbase, it must be mature
                    if (prevOutput.IsCoinbase
                        && chainedHeader.Height - prevOutput.BlockHeight < COINBASE_MATURITY)
                    {
                        throw new ValidationException(chainedHeader.Hash);
                    }

                    // non-coinbase txes must not have coinbase prev tx output key (txHash: 0, outputIndex: -1)
                    if (input.PrevTxOutputKey.TxOutputIndex == uint.MaxValue && input.PrevTxOutputKey.TxHash == UInt256.Zero)
                        throw new ValidationException(chainedHeader.Hash);

                    // tally
                    txTotalInputValue += prevOutput.Value;
                }

                // tally
                txSigOpCount += CountLegacySigOps(input.ScriptSignature, accurate: false);
                if (!tx.IsCoinbase && strictPayToScriptHash)
                    txSigOpCount += CountP2SHSigOps(validatableTx, inputIndex);
            }

            // validate & tally outputs
            for (var outputIndex = 0; outputIndex < tx.Outputs.Length; outputIndex++)
            {
                var output = tx.Outputs[outputIndex];

                // must not have any negative money value outputs
                if (unchecked((long)output.Value) < 0)
                    throw new ValidationException(chainedHeader.Hash);
                // must not have any outputs with a value greater than max money
                else if (output.Value > MAX_MONEY)
                    throw new ValidationException(chainedHeader.Hash);

                // tally
                if (!tx.IsCoinbase)
                    txTotalOutputValue += output.Value;
                txSigOpCount += CountLegacySigOps(output.ScriptPublicKey, accurate: false);
            }

            // must not have a total output value greater than max money
            if (txTotalOutputValue > MAX_MONEY)
                throw new ValidationException(chainedHeader.Hash);

            // validate non-coinbase fees
            long txFeeValue;
            if (!validatableTx.IsCoinbase)
            {
                // ensure that output amount isn't greater than input amount
                if (txTotalOutputValue > txTotalInputValue)
                    throw new ValidationException(chainedHeader.Hash, $"Failing tx {tx.Hash}: Transaction output value is greater than input value");

                // calculate fee value (unspent amount)
                txFeeValue = (long)txTotalInputValue - (long)txTotalOutputValue;
                Debug.Assert(txFeeValue >= 0);
            }
            else
                txFeeValue = 0;


            // block tallies
            if (validatableTx.IsCoinbase)
                blockTally.CoinbaseTx = validatableTx;
            blockTally.TxCount++;
            blockTally.TotalFees += txFeeValue;
            blockTally.TotalSigOpCount += txSigOpCount;
            // re-encode transaction for block size calculation so it is optimal length
            blockTally.BlockSize +=
                DataEncoder.EncodeTransaction(validatableTx.Transaction).TxBytes.Length;

            //TODO
            if (blockTally.TotalSigOpCount > MAX_BLOCK_SIGOPS)
                throw new ValidationException(chainedHeader.Hash);
            //TODO
            else if (blockTally.BlockSize + DataEncoder.VarIntSize((uint)blockTally.TxCount) > MAX_BLOCK_SIZE)
                throw new ValidationException(chainedHeader.Hash);

            // all validation has passed
        }

        public void ValidateTransaction(Chain newChain, ValidatableTx validatableTx)
        {
            // TODO any expensive validation can go here
        }

        public void ValidationTransactionScript(Chain newChain, BlockTx tx, TxInput txInput, int txInputIndex, PrevTxOutput prevTxOutput)
        {
            var chainedHeader = newChain.LastBlock;

            // BIP16 didn't become active until Apr 1 2012
            var nBIP16SwitchTime = DateTimeOffset.FromUnixTimeSeconds(1333238400U);
            var strictPayToScriptHash = chainedHeader.Time >= nBIP16SwitchTime;

            var flags = strictPayToScriptHash ? verify_flags_type.verify_flags_p2sh : verify_flags_type.verify_flags_none;

            // Start enforcing the DERSIG (BIP66) rules, for block.nVersion=3 blocks,
            // when 75% of the network has upgraded:
            if (chainedHeader.Version >= 3
                && IsSuperMajority(3, newChain, ChainParams.MajorityEnforceBlockUpgrade))
            {
                flags |= verify_flags_type.verify_flags_dersig;
            }

            // Start enforcing CHECKLOCKTIMEVERIFY, (BIP65) for block.nVersion=4
            // blocks, when 75% of the network has upgraded:
            if (chainedHeader.Version >= 4
                && IsSuperMajority(4, newChain, ChainParams.MajorityEnforceBlockUpgrade))
            {
                flags |= verify_flags_type.verify_flags_checklocktimeverify;
            }

            var result = LibbitcoinConsensus.VerifyScript(
                tx.TxBytes,
                prevTxOutput.ScriptPublicKey,
                txInputIndex,
                flags);

            if (!result)
            {
                logger.Debug($"Script did not pass in block: {chainedHeader.Hash}, tx: {tx.Index}, {tx.Hash}, input: {txInputIndex}");
                throw new ValidationException(chainedHeader.Hash);
            }
        }

        public void PostValidateBlock(Chain newChain, object finalTally)
        {
            var chainedHeader = newChain.LastBlock;
            var blockTally = (BlockTally)finalTally;

            // ensure there is at least 1 transaction
            if (blockTally.TxCount == 0)
                throw new ValidationException(chainedHeader.Hash,
                    $"Failing block {chainedHeader.Hash} at height {chainedHeader.Height}: Zero transactions present");

            // ensure fees aren't negative (should be caught earlier)
            if (blockTally.TotalFees < 0)
                throw new ValidationException(chainedHeader.Hash);

            // calculate the expected reward in coinbase
            var subsidy = (ulong)(50 * SATOSHI_PER_BTC);
            if (chainedHeader.Height / 210000 <= 32)
                subsidy /= (ulong)Math.Pow(2, chainedHeader.Height / 210000);

            // calculate the expected reward in coinbase
            var expectedReward = subsidy + (ulong)blockTally.TotalFees;

            // calculate the actual reward in coinbase
            var actualReward = blockTally.CoinbaseTx.Transaction.Outputs.Sum(x => x.Value);

            // ensure coinbase has correct reward
            if (actualReward > expectedReward)
            {
                throw new ValidationException(chainedHeader.Hash,
                    $"Failing block {chainedHeader.Hash} at height {chainedHeader.Height}: Coinbase value is greater than reward + fees");
            }
        }

        //TODO not needed, but hanging onto
        //public double TargetToDifficulty(UInt256 target)
        //{
        //    // difficulty is HighestTarget / target
        //    // since these are 256-bit numbers, use division trick for BigIntegers
        //    return Math.Exp(BigInteger.Log(ChainParams.HighestTarget.ToBigInteger()) - BigInteger.Log(target.ToBigInteger()));
        //}

        //public UInt256 DifficultyToTarget(double difficulty)
        //{
        //    // implementation is equivalent of HighestTarget / difficulty

        //    // multiply difficulty and HighestTarget by a scale so that the decimal portion can be fed into a BigInteger
        //    var scale = 0x100000000L;
        //    var highestTargetScaled = (BigInteger)ChainParams.HighestTarget * scale;
        //    var difficultyScaled = (BigInteger)(difficulty * scale);

        //    // do the division
        //    var target = highestTargetScaled / difficultyScaled;

        //    // get the resulting target bytes, taking only the 3 most significant
        //    var targetBytes = target.ToByteArray();
        //    targetBytes = new byte[targetBytes.Length - 3].Concat(targetBytes.Skip(targetBytes.Length - 3).ToArray());

        //    // return the target
        //    return new UInt256(targetBytes);
        //}

        public uint GetRequiredNextBits(Chain chain)
        {
            var powLimitCompact = DataCalculator.TargetToBits(ChainParams.HighestTarget);

            if (chain.Height == 0)
                return powLimitCompact;

            var prevHeader = chain.Blocks[chain.Height - 1];

            if (ChainParams.PowNoRetargeting)
                return prevHeader.Bits;

            // not on an adjustment interval, use previous block's target
            if (chain.Height % ChainParams.DifficultyInterval != 0)
            {
                if (!ChainParams.AllowMininimumDifficultyBlocks)
                    return prevHeader.Bits;
                else
                {
                    // Special difficulty rule for testnet:
                    // If the new block's timestamp is more than 2* 10 minutes
                    // then allow mining of a min-difficulty block.
                    var currentHeader = chain.LastBlock;
                    if (currentHeader.Time > prevHeader.Time + TimeSpan.FromTicks(ChainParams.PowTargetSpacing.Ticks * 2))
                        return powLimitCompact;
                    else
                    {
                        // Return the last non-special-min-difficulty-rules-block
                        var header = prevHeader;
                        while (header.Height > 0
                            && header.Height % ChainParams.DifficultyInterval != 0
                            && header.Bits == powLimitCompact)
                        {
                            header = chain.Blocks[header.Height - 1];
                        }
                        return header.Bits;
                    }
                }
            }
            // on an adjustment interval, calculate the required next target
            else
            {
                // get the block difficultyInterval blocks ago
                var prevIntervalHeight = prevHeader.Height - (ChainParams.DifficultyInterval - 1);
                var prevIntervalHeader = chain.Blocks[prevIntervalHeight];

                var actualTimespan = (uint)(prevHeader.Time - prevIntervalHeader.Time).TotalSeconds;
                var targetTimespan = (uint)(ChainParams.DifficultyTargetTimespan).TotalSeconds;

                // limit adjustment to 4x or 1/4x
                if (actualTimespan < targetTimespan / 4)
                    actualTimespan = targetTimespan / 4;
                else if (actualTimespan > targetTimespan * 4)
                    actualTimespan = targetTimespan * 4;

                // calculate the new target
                var target = prevHeader.BlockHeader.CalculateTarget();
                target *= (UInt256)actualTimespan;
                target /= (UInt256)targetTimespan;

                // make sure target isn't too high (too low difficulty)
                if (target > ChainParams.HighestTarget)
                    target = ChainParams.HighestTarget;

                return DataCalculator.TargetToBits(target);
            }
        }

        private int CountLegacySigOps(ImmutableArray<byte> script, bool accurate)
        {
            var sigOpCount = 0;

            var index = 0;
            while (index < script.Length)
            {
                ScriptOp op;
                if (!GetOp(script, ref index, out op))
                    break;

                switch (op)
                {
                    case ScriptOp.OP_CHECKSIG:
                    case ScriptOp.OP_CHECKSIGVERIFY:
                        sigOpCount++;
                        break;

                    case ScriptOp.OP_CHECKMULTISIG:
                    case ScriptOp.OP_CHECKMULTISIGVERIFY:
                        //TODO
                        var prevOpCode = index >= 2 ? script[index - 2] : (byte)ScriptOp.OP_INVALIDOPCODE;
                        if (accurate && prevOpCode >= (byte)ScriptOp.OP_1 && prevOpCode <= (byte)ScriptOp.OP_16)
                            sigOpCount += prevOpCode - ((byte)ScriptOp.OP_1 - 1);
                        else
                            sigOpCount += MAX_PUBKEYS_PER_MULTISIG;

                        break;
                }
            }

            return sigOpCount;
        }

        private int CountP2SHSigOps(ValidatableTx validatableTx)
        {
            var sigOpCount = 0;

            if (validatableTx.IsCoinbase)
                return 0;

            for (var inputIndex = 0; inputIndex < validatableTx.Transaction.Inputs.Length; inputIndex++)
            {
                sigOpCount += CountP2SHSigOps(validatableTx, inputIndex);
            }

            return sigOpCount;
        }

        private int CountP2SHSigOps(ValidatableTx validatableTx, int inputIndex)
        {
            if (validatableTx.IsCoinbase)
                return 0;

            var prevTxOutput = validatableTx.PrevTxOutputs[inputIndex];
            if (prevTxOutput.IsPayToScriptHash())
            {
                var script = validatableTx.Transaction.Inputs[inputIndex].ScriptSignature;

                ImmutableArray<byte>? data = null;

                // find the last item pushed onto the stack in the P2SH script signature...
                var index = 0;
                while (index < script.Length)
                {
                    ScriptOp op;
                    if (!GetOp(script, ref index, out op, out data))
                        return 0;
                    else if (op > ScriptOp.OP_16)
                        return 0;
                }

                if (data == null)
                    return 0;

                // ...and count its sig ops
                return CountLegacySigOps(data.Value, accurate: true);
            }
            else
                return 0;
        }

        private bool GetOp(ImmutableArray<byte> script, ref int index, out ScriptOp op)
        {
            ImmutableArray<byte>? data;
            return GetOp(script, ref index, out op, out data, readData: false);
        }

        private bool GetOp(ImmutableArray<byte> script, ref int index, out ScriptOp op, out ImmutableArray<byte>? data)
        {
            return GetOp(script, ref index, out op, out data, readData: true);
        }

        private bool GetOp(ImmutableArray<byte> script, ref int index, out ScriptOp op, out ImmutableArray<byte>? data, bool readData)
        {
            op = ScriptOp.OP_INVALIDOPCODE;
            data = null;

            if (index + 1 > script.Length)
                return false;

            var opByte = script[index++];
            var currentOp = (ScriptOp)opByte;

            if (currentOp <= ScriptOp.OP_PUSHDATA4)
            {
                //OP_PUSHBYTES1-75
                uint dataLength;
                if (currentOp < ScriptOp.OP_PUSHDATA1)
                {
                    dataLength = opByte;
                }
                else if (currentOp == ScriptOp.OP_PUSHDATA1)
                {
                    if (index + 1 > script.Length)
                        return false;

                    dataLength = script[index++];
                }
                else if (currentOp == ScriptOp.OP_PUSHDATA2)
                {
                    if (index + 2 > script.Length)
                        return false;

                    dataLength = (uint)script[index++] + ((uint)script[index++] << 8);
                }
                else if (currentOp == ScriptOp.OP_PUSHDATA4)
                {
                    if (index + 4 > script.Length)
                        return false;

                    dataLength = (uint)script[index++] + ((uint)script[index++] << 8) + ((uint)script[index++] << 16) + ((uint)script[index++] << 24);
                }
                else
                {
                    dataLength = 0;
                    Debug.Assert(false);
                }

                if ((ulong)index + dataLength > (uint)script.Length)
                    return false;

                if (readData)
                    data = ImmutableArray.Create(script, index, (int)dataLength);

                index += (int)dataLength;
            }

            op = currentOp;
            return true;
        }

        private DateTimeOffset GetMedianPrevHeaderTime(Chain chain, int height)
        {
            if (height == 0)
                return DateTimeOffset.FromUnixTimeSeconds(0);

            var medianTimeSpan = Math.Min(11, height);

            var prevHeaderTimes = chain.Blocks.GetRange(height - medianTimeSpan, medianTimeSpan)
                //TODO pull tester doesn't fail if the sort step is missed
                .OrderBy(x => x.Time).ToList();

            return prevHeaderTimes[prevHeaderTimes.Count / 2].Time;
        }

        private bool IsFinal(Transaction tx, int blockHeight, long blockTime)
        {
            // no tx lock time specified, transaction is final
            if (tx.LockTime == 0)
                return true;

            // lock time is compared by block height before LOCKTIME_THRESHOLD, block time afterwards
            var blockLockTime = (tx.LockTime < LOCKTIME_THRESHOLD ? blockHeight : blockTime);

            // if block's time is after tx's lock time, transaction is final
            if (blockLockTime >= tx.LockTime)
                return true;

            // if tx's lock time hasn't yet been reached, the transaction is final if all its inputs are final
            return tx.Inputs.All(x => IsFinal(x));
        }

        private bool IsFinal(TxInput input)
        {
            return input.Sequence == uint.MaxValue;
        }

        private bool IsSuperMajority(int minVersion, Chain newChain, int requiredCount)
        {
            if (newChain.Height == 0)
                return false;

            var count = 0;
            var metVersionCount = 0;
            for (var i = newChain.Height - 1; i >= 0 && count < ChainParams.MajorityWindow; i--)
            {
                if (newChain.Blocks[i].Version >= minVersion)
                    metVersionCount++;

                if (metVersionCount >= requiredCount)
                    return true;

                count++;
            }

            return false;
        }

        private ImmutableArray<byte> GetPushInt64Script(long value)
        {
            var script = new ScriptBuilder();

            // push 0 onto the stack using OP_0
            if (value == 0)
            {
                script.WriteOp(ScriptOp.OP_0);
            }
            // push -1 onto the stack using OP_1NEGATE
            else if (value == -1)
            {
                script.WriteOp(ScriptOp.OP_1NEGATE);
            }
            // push 1-16 onto the stack using OP_1 to OP_16
            else if (value >= 1 && value <= 16)
            {
                script.WriteOp((ScriptOp)(value + ((long)ScriptOp.OP_1 - 1)));
            }
            else
            {
                var valueBytes = SerializeScriptValue(value);
                script.WritePushData(valueBytes);
            }

            return script.GetScript().ToImmutableArray();
        }

        private byte[] SerializeScriptValue(long value)
        {
            if (value == 0)
                return new byte[0];

            var result = new List<byte>();

            var neg = value < 0;
            var absvalue = (ulong)(neg ? -value : value);

            while (absvalue != 0)
            {
                result.Add((byte)(absvalue & 0xff));
                absvalue >>= 8;
            }

            //    - If the most significant byte is >= 0x80 and the value is positive, push a
            //    new zero-byte to make the significant byte < 0x80 again.

            //    - If the most significant byte is >= 0x80 and the value is negative, push a
            //    new 0x80 byte that will be popped off when converting to an integral.

            //    - If the most significant byte is < 0x80 and the value is negative, add
            //    0x80 to it, since it will be subtracted and interpreted as a negative when
            //    converting to an integral.

            if ((result.Last() & 0x80) != 0)
                result.Add((byte)(neg ? 0x80 : 0));
            else if (neg)
                result[result.Count - 1] = (byte)(result.Last() | 0x80);

            return result.ToArray();
        }

        private class BlockTally
        {
            public ValidatableTx CoinbaseTx { get; set; }

            public int TxCount { get; set; }

            public long TotalFees { get; set; }

            public int TotalSigOpCount { get; set; }

            public int BlockSize { get; set; }

            public long LockTimeCutoff { get; set; }
        }
    }
}
