using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using BitSharp.Core.ExtensionMethods;
using NLog;
using System;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Text;

namespace BitSharp.Core
{
    public class DataDecoder
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();

        public static Block DecodeBlock(BinaryReader reader)
        {
            var header = DecodeBlockHeader(null, reader.ReadExactly(80));

            var blockTxesCount = reader.ReadVarInt().ToIntChecked();
            var blockTxes = ImmutableArray.CreateBuilder<BlockTx>(blockTxesCount);
            for (var i = 0; i < blockTxesCount; i++)
            {
                var txBytes = ReadTransaction(reader);
                var encodedTx = DecodeEncodedTx(null, txBytes);
                var blockTx = new BlockTx(i, encodedTx);

                blockTxes.Add(blockTx);

            }

            return new Block(header, blockTxes.MoveToImmutable());
        }

        public static Block DecodeBlock(UInt256 blockHash, byte[] buffer, int offset = 0)
        {
            return DecodeBlock(blockHash, buffer, ref offset);
        }

        public static Block DecodeBlock(UInt256 blockHash, byte[] buffer, ref int offset)
        {
            var header = DecodeBlockHeader(blockHash, buffer, ref offset);

            var blockTxesCount = buffer.ReadVarInt(ref offset).ToIntChecked();
            var blockTxes = ImmutableArray.CreateBuilder<BlockTx>(blockTxesCount);
            for (var i = 0; i < blockTxesCount; i++)
            {
                var encodedTx = DecodeEncodedTx(null, buffer, ref offset);
                var blockTx = new BlockTx(i, encodedTx);

                blockTxes.Add(blockTx);

            }

            return new Block(header, blockTxes.MoveToImmutable());
        }

        public static BlockHeader DecodeBlockHeader(UInt256 blockHash, byte[] buffer, int offset = 0)
        {
            return DecodeBlockHeader(blockHash, buffer, ref offset);
        }

        public static BlockHeader DecodeBlockHeader(UInt256 blockHash, byte[] buffer, ref int offset)
        {
            var initialOffset = offset;

            var version = DecodeUInt32(buffer, ref offset);
            var previousBlock = DecodeUInt256(buffer, ref offset);
            var merkleRoot = DecodeUInt256(buffer, ref offset);
            var time = DateTimeOffset.FromUnixTimeSeconds(DecodeUInt32(buffer, ref offset));
            var bits = DecodeUInt32(buffer, ref offset);
            var nonce = DecodeUInt32(buffer, ref offset);

            blockHash = blockHash ?? new UInt256(SHA256Static.ComputeDoubleHash(buffer, initialOffset, 80));

            return new BlockHeader(version, previousBlock, merkleRoot, time, bits, nonce, blockHash);
        }

        public static BigInteger DecodeTotalWork(byte[] buffer)
        {
            var totalWorkBytes = new byte[64];
            Buffer.BlockCopy(buffer, 0, totalWorkBytes, 0, 64);
            Array.Reverse(totalWorkBytes);
            return new BigInteger(totalWorkBytes);
        }

        public static ChainedHeader DecodeChainedHeader(byte[] buffer)
        {
            var offset = 0;
            var blockHash = DecodeUInt256(buffer, ref offset);
            var blockHeader = DecodeBlockHeader(blockHash, buffer, ref offset);
            var height = DecodeInt32(buffer, ref offset);
            var totalWork = new BigInteger(buffer.ReadVarBytes(ref offset));
            var dateSeen = new DateTimeOffset(DecodeInt64(buffer, ref offset), TimeSpan.Zero);

            return new ChainedHeader(blockHeader, height, totalWork, dateSeen);
        }

        public static byte[] ReadTransaction(BinaryReader reader)
        {
            var txBytes = new byte[1024];
            var offset = 0;

            // read version
            reader.ReadExactly(txBytes, offset, 4);
            offset += 4;

            // read inputs
            var inputCount = reader.ReadVarInt(ref txBytes, ref offset).ToIntChecked();
            for (var i = 0; i < inputCount; i++)
            {
                // read prevTxHash and prevTxOutputIndex
                SizeAtLeast(ref txBytes, offset + 36);
                reader.ReadExactly(txBytes, offset, 36);
                offset += 36;

                // read scriptSignatureLength
                var scriptSignatureLength = reader.ReadVarInt(ref txBytes, ref offset).ToIntChecked();

                // read scriptSignature
                SizeAtLeast(ref txBytes, offset + scriptSignatureLength);
                reader.ReadExactly(txBytes, offset, scriptSignatureLength);
                offset += scriptSignatureLength;

                // read sequence
                SizeAtLeast(ref txBytes, offset + 4);
                reader.ReadExactly(txBytes, offset, 4);
                offset += 4;
            }

            // read outputs
            var outputCount = reader.ReadVarInt(ref txBytes, ref offset).ToIntChecked();
            for (var i = 0; i < outputCount; i++)
            {
                // read value
                SizeAtLeast(ref txBytes, offset + 8);
                reader.ReadExactly(txBytes, offset, 8);
                offset += 8;

                // read scriptPublicKeyLength
                var scriptPublicKeyLength = reader.ReadVarInt(ref txBytes, ref offset).ToIntChecked();

                // read scriptPublicKey
                SizeAtLeast(ref txBytes, offset + scriptPublicKeyLength);
                reader.ReadExactly(txBytes, offset, scriptPublicKeyLength);
                offset += scriptPublicKeyLength;
            }

            // read lockTime
            SizeAtLeast(ref txBytes, offset + 4);
            reader.ReadExactly(txBytes, offset, 4);
            offset += 4;

            // resize raw tx bytes to final size
            Array.Resize(ref txBytes, offset);

            return txBytes;
        }

        public static DecodedTx DecodeEncodedTx(UInt256 txHash, byte[] buffer, int offset = 0)
        {
            return DecodeEncodedTx(txHash, buffer, ref offset);
        }

        public static DecodedTx DecodeEncodedTx(UInt256 txHash, byte[] buffer, ref int offset)
        {
            var initialOffset = offset;

            var tx = DecodeTransaction(txHash, buffer, ref offset);
            var txBytes = ImmutableArray.Create(buffer, initialOffset, offset - initialOffset);

            return new DecodedTx(txBytes, tx);
        }

        public static Transaction DecodeTransaction(UInt256 txHash, byte[] buffer, int offset = 0)
        {
            return DecodeTransaction(txHash, buffer, ref offset);
        }

        public static Transaction DecodeTransaction(UInt256 txHash, byte[] buffer, ref int offset)
        {
            var initialOffset = offset;

            // read version
            var version = DecodeUInt32(buffer, ref offset);

            // read inputs
            var inputs = DecodeTxInputList(buffer, ref offset);

            // read outputs
            var outputs = DecodeTxOutputList(buffer, ref offset);

            // read lockTime
            var lockTime = DecodeUInt32(buffer, ref offset);

            txHash = txHash ?? new UInt256(SHA256Static.ComputeDoubleHash(buffer, initialOffset, offset - initialOffset));

            return new Transaction(version, inputs, outputs, lockTime, txHash);

        }

        public static TxInput DecodeTxInput(byte[] buffer, int offset = 0)
        {
            return DecodeTxInput(buffer, ref offset);
        }

        public static TxInput DecodeTxInput(byte[] buffer, ref int offset)
        {
            var prevTxHash = DecodeUInt256(buffer, ref offset);
            var prevTxOutputIndex = DecodeUInt32(buffer, ref offset);
            var scriptSignature = buffer.ReadVarBytesImmutable(ref offset);
            var sequence = DecodeUInt32(buffer, ref offset);

            return new TxInput(prevTxHash, prevTxOutputIndex, scriptSignature, sequence);
        }

        public static ImmutableArray<TxInput> DecodeTxInputList(byte[] buffer, int offset = 0)
        {
            return DecodeTxInputList(buffer, ref offset);
        }

        public static ImmutableArray<TxInput> DecodeTxInputList(byte[] buffer, ref int offset)
        {
            var count = buffer.ReadVarInt(ref offset).ToIntChecked();

            // read inputs
            var inputs = ImmutableArray.CreateBuilder<TxInput>(count);
            for (var i = 0; i < count; i++)
            {
                var input = DecodeTxInput(buffer, ref offset);
                inputs.Add(input);
            }

            return inputs.ToImmutable();
        }

        public static TxOutput DecodeTxOutput(byte[] buffer, int offset = 0)
        {
            return DecodeTxOutput(buffer, ref offset);
        }

        public static TxOutput DecodeTxOutput(byte[] buffer, ref int offset)
        {
            var value = DecodeUInt64(buffer, ref offset);
            var scriptPublicKey = buffer.ReadVarBytesImmutable(ref offset);

            return new TxOutput(value, scriptPublicKey);
        }

        public static ImmutableArray<TxOutput> DecodeTxOutputList(byte[] buffer, int offset = 0)
        {
            return DecodeTxOutputList(buffer, ref offset);
        }

        public static ImmutableArray<TxOutput> DecodeTxOutputList(byte[] buffer, ref int offset)
        {
            var count = buffer.ReadVarInt(ref offset).ToIntChecked();

            // read outputs
            var outputs = ImmutableArray.CreateBuilder<TxOutput>(count);
            for (var i = 0; i < count; i++)
            {
                var output = DecodeTxOutput(buffer, ref offset);
                outputs.Add(output);
            }

            return outputs.ToImmutable();
        }

        public static PrevTxOutput DecodePrevTxOutput(byte[] buffer, int offset = 0)
        {
            return DecodePrevTxOutput(buffer, ref offset);
        }

        public static PrevTxOutput DecodePrevTxOutput(byte[] buffer, ref int offset)
        {
            var value = DecodeUInt64(buffer, ref offset);
            var scriptPublicKey = buffer.ReadVarBytesImmutable(ref offset);
            var blockHeight = DecodeInt32(buffer, ref offset);
            var txIndex = DecodeInt32(buffer, ref offset);
            var txVersion = DecodeUInt32(buffer, ref offset);
            var isCoinbase = DecodeBool(buffer, ref offset);

            return new PrevTxOutput(value, scriptPublicKey, blockHeight, txIndex, txVersion, isCoinbase);
        }

        public static ImmutableArray<PrevTxOutput> DecodePrevTxOutputList(byte[] buffer, int offset = 0)
        {
            return DecodePrevTxOutputList(buffer, ref offset);
        }

        public static ImmutableArray<PrevTxOutput> DecodePrevTxOutputList(byte[] buffer, ref int offset)
        {
            var count = buffer.ReadVarInt(ref offset).ToIntChecked();

            var prevTxOutputs = ImmutableArray.CreateBuilder<PrevTxOutput>(count);
            for (var i = 0; i < count; i++)
            {
                var prevTxOutput = DecodePrevTxOutput(buffer, ref offset);
                prevTxOutputs.Add(prevTxOutput);
            }

            return prevTxOutputs.ToImmutable();
        }

        public static UnspentTx DecodeUnspentTx(byte[] buffer, int offset = 0)
        {
            return DecodeUnspentTx(buffer, ref offset);
        }

        public static UnspentTx DecodeUnspentTx(byte[] buffer, ref int offset)
        {
            var txHash = DecodeUInt256(buffer, ref offset);
            var blockIndex = DecodeInt32(buffer, ref offset);
            var index = DecodeInt32(buffer, ref offset);
            var txVersion = DecodeUInt32(buffer, ref offset);
            var isCoinbase = DecodeBool(buffer, ref offset);

            var outputStateBits = buffer.ReadVarBytes(ref offset);
            var outputStateLength = DecodeInt32(buffer, ref offset);
            var outputStates = new OutputStates(outputStateBits, outputStateLength);

            var txOutputs = DecodeTxOutputList(buffer, ref offset);

            return new UnspentTx(txHash, blockIndex, index, txVersion, isCoinbase, outputStates, txOutputs);
        }

        public static SpentTx DecodeSpentTx(byte[] buffer, int offset = 0)
        {
            return DecodeSpentTx(buffer, ref offset);
        }

        public static SpentTx DecodeSpentTx(byte[] buffer, ref int offset)
        {
            var txHash = DecodeUInt256(buffer, ref offset);
            var confirmedBlockIndex = DecodeInt32(buffer, ref offset);
            var txIndex = DecodeInt32(buffer, ref offset);

            return new SpentTx(txHash, confirmedBlockIndex, txIndex);
        }

        public static UnmintedTx DecodeUnmintedTx(byte[] buffer, int offset = 0)
        {
            return DecodeUnmintedTx(buffer, ref offset);
        }

        public static UnmintedTx DecodeUnmintedTx(byte[] buffer, ref int offset)
        {
            var txHash = DecodeUInt256(buffer, ref offset);
            var prevTxOutputs = DecodePrevTxOutputList(buffer, ref offset);

            return new UnmintedTx(txHash, prevTxOutputs);
        }

        public static IImmutableList<UnmintedTx> DecodeUnmintedTxList(byte[] buffer, int offset = 0)
        {
            return DecodeUnmintedTxList(buffer, ref offset);
        }

        public static IImmutableList<UnmintedTx> DecodeUnmintedTxList(byte[] buffer, ref int offset)
        {
            var count = buffer.ReadVarInt(ref offset).ToIntChecked();

            var unmintedTxes = ImmutableArray.CreateBuilder<UnmintedTx>(count);
            for (var i = 0; i < count; i++)
            {
                var unmintedTx = DecodeUnmintedTx(buffer, ref offset);
                unmintedTxes.Add(unmintedTx);
            }

            return unmintedTxes.ToImmutable();
        }

        public static OutputStates DecodeOutputStates(byte[] buffer)
        {
            var length = BitConverter.ToInt32(buffer, 0);
            var outputStateBytes = new byte[buffer.Length - 4];
            Buffer.BlockCopy(buffer, 4, outputStateBytes, 0, buffer.Length - 4);

            return new OutputStates(outputStateBytes, length);
        }

        public static BlockTxNode DecodeBlockTxNode(byte[] buffer, bool skipTxBytes = false)
        {
            var offset = 0;
            var index = DecodeInt32(buffer, ref offset);
            var depth = DecodeInt32(buffer, ref offset);
            var hash = DecodeUInt256(buffer, ref offset);
            var pruned = DecodeBool(buffer, ref offset);

            ImmutableArray<byte>? txBytes;
            if (!skipTxBytes)
            {
                var bytesRemaining = buffer.Length - offset;
                txBytes = ImmutableArray.Create(buffer, offset, bytesRemaining);
            }
            else
                txBytes = null;

            return new BlockTxNode(index, depth, hash, pruned, txBytes);
        }

        public static BlockSpentTxes DecodeBlockSpentTxes(byte[] buffer, int offset = 0)
        {
            return DecodeBlockSpentTxes(buffer, ref offset);
        }

        public static BlockSpentTxes DecodeBlockSpentTxes(byte[] buffer, ref int offset)
        {
            var count = buffer.ReadVarInt(ref offset).ToIntChecked();

            var blockSpentTxesBuilder = new BlockSpentTxesBuilder();
            for (var i = 0; i < count; i++)
            {
                var spentTx = DecodeSpentTx(buffer, ref offset);
                blockSpentTxesBuilder.AddSpentTx(spentTx);
            }

            return blockSpentTxesBuilder.ToImmutable();
        }

        public static string DecodeVarString(byte[] buffer, int offset = 0)
        {
            return DecodeVarString(buffer, ref offset);
        }

        public static string DecodeVarString(byte[] buffer, ref int offset)
        {
            var rawBytes = buffer.ReadVarBytes(ref offset);
            return Encoding.ASCII.GetString(rawBytes);
        }

        public static string DecodeFixedString(byte[] buffer, int offset = 0, int? length = null)
        {
            if (length == null)
                length = buffer.Length;
            return DecodeFixedString(buffer, ref offset, length.Value);
        }

        public static string DecodeFixedString(byte[] buffer, ref int offset, int length)
        {
            var encoded = new byte[length];
            Buffer.BlockCopy(buffer, offset, encoded, 0, length);

            // ignore trailing null bytes in a fixed length string
            var encodedTrimmed = encoded.TakeWhile(x => x != 0).ToArray();
            return Encoding.ASCII.GetString(encodedTrimmed);
        }

        internal static void SizeAtLeast(ref byte[] buffer, int minLength)
        {
            if (buffer.Length < minLength)
                Array.Resize(ref buffer, ((minLength + 1023) / 1024) * 1024);
        }

        public static int DecodeInt32(byte[] buffer, ref int offset)
        {
            var value = Bits.ToInt32(buffer, offset);
            offset += 4;
            return value;
        }

        public static uint DecodeUInt32(byte[] buffer, ref int offset)
        {
            var value = Bits.ToUInt32(buffer, offset);
            offset += 4;
            return value;
        }

        public static long DecodeInt64(byte[] buffer, ref int offset)
        {
            var value = Bits.ToInt64(buffer, offset);
            offset += 8;
            return value;
        }

        public static ulong DecodeUInt64(byte[] buffer, ref int offset)
        {
            var value = Bits.ToUInt64(buffer, offset);
            offset += 8;
            return value;
        }

        public static UInt256 DecodeUInt256(byte[] buffer, ref int offset)
        {
            var value = new UInt256(buffer, offset);
            offset += 32;
            return value;
        }

        private static bool DecodeBool(byte[] buffer, ref int offset)
        {
            var value = buffer[offset] != 0;
            offset += 1;
            return value;
        }

        public static int DecodeInt32(byte[] buffer, int offset = 0)
        {
            return DecodeInt32(buffer, ref offset);
        }

        public static uint DecodeUInt32(byte[] buffer, int offset = 0)
        {
            return DecodeUInt32(buffer, ref offset);
        }

        public static long DecodeInt64(byte[] buffer, int offset = 0)
        {
            return DecodeInt64(buffer, ref offset);
        }

        public static ulong DecodeUInt64(byte[] buffer, int offset = 0)
        {
            return DecodeUInt64(buffer, ref offset);
        }

        public static UInt256 DecodeUInt256(byte[] buffer, int offset = 0)
        {
            return DecodeUInt256(buffer, ref offset);
        }

        private static bool DecodeBool(byte[] buffer, int offset = 0)
        {
            return DecodeBool(buffer, ref offset);
        }
    }
}
