using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using System;
using System.Collections;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Core.Test
{
    public class RandomDataOptions
    {
        public int? MinimumBlockCount { get; set; }
        public int? BlockCount { get; set; }
        public int? TransactionCount { get; set; }
        public int? TxInputCount { get; set; }
        public int? TxOutputCount { get; set; }
        public int? ScriptSignatureSize { get; set; }
        public int? ScriptPublicKeySize { get; set; }
    }

    public static class RandomData
    {
        private static readonly Random random = new Random();

        public static Block RandomBlock(RandomDataOptions options = default(RandomDataOptions))
        {
            return new Block
            (
                header: RandomBlockHeader(),
                transactions: Enumerable.Range(0, random.Next((options != null ? options.TransactionCount : null) ?? 100)).Select(x => RandomTransaction()).ToImmutableArray()
            );
        }

        public static BlockHeader RandomBlockHeader(RandomDataOptions options = default(RandomDataOptions))
        {
            return new BlockHeader
            (
                version: random.NextUInt32(),
                previousBlock: random.NextUInt256(),
                merkleRoot: random.NextUInt256(),
                time: random.NextUInt32(),
                bits: random.NextUInt32(),
                nonce: random.NextUInt32()
            );
        }

        public static Transaction RandomTransaction(RandomDataOptions options = default(RandomDataOptions))
        {
            return new Transaction
            (
                version: random.NextUInt32(),
                inputs: Enumerable.Range(0, random.Next((options != null ? options.TxInputCount : null) ?? 100)).Select(x => RandomTxInput()).ToImmutableArray(),
                outputs: Enumerable.Range(0, random.Next((options != null ? options.TxOutputCount : null) ?? 100)).Select(x => RandomTxOutput()).ToImmutableArray(),
                lockTime: random.NextUInt32()
            );
        }

        public static TxInput RandomTxInput(RandomDataOptions options = default(RandomDataOptions))
        {
            return new TxInput
            (
                previousTxOutputKey: new TxOutputKey
                (
                    txHash: random.NextUInt32(),
                    txOutputIndex: random.NextUInt32()
                ),
                scriptSignature: random.NextBytes(random.Next((options != null ? options.ScriptSignatureSize : null) ?? 100)),
                sequence: random.NextUInt32()
            );
        }

        public static TxOutput RandomTxOutput(RandomDataOptions options = default(RandomDataOptions))
        {
            return new TxOutput
            (
                value: random.NextUInt64(),
                scriptPublicKey: random.NextBytes(random.Next((options != null ? options.ScriptPublicKeySize : null) ?? 100))
            );
        }

        public static ChainedHeader RandomChainedHeader(RandomDataOptions options = default(RandomDataOptions))
        {
            return new ChainedHeader
            (
                blockHeader: RandomBlockHeader(options),
                height: Math.Abs(random.Next()),
                totalWork: random.NextUBigIntegerBytes(64)
            );
        }

        public static UnspentTx RandomUnspentTx(RandomDataOptions options = default(RandomDataOptions))
        {
            return new UnspentTx
            (
                txHash: random.NextUInt256(),
                blockIndex: random.Next(),
                txIndex: random.Next(),
                outputStates: new OutputStates(random.NextImmutableBitArray(random.Next((options != null ? options.TxOutputCount : null) ?? 100)))
            );
        }

        public static TxOutputKey RandomTxOutputKey()
        {
            return new TxOutputKey
            (
                txHash: random.NextUInt256(),
                txOutputIndex: random.NextUInt32()
            );
        }

        public static ImmutableBitArray NextImmutableBitArray(this Random random, int length)
        {
            var bitArray = new BitArray(length);
            for (var i = 0; i < length; i++)
                bitArray[i] = random.NextBool();

            return bitArray.ToImmutableBitArray();
        }
    }
}
