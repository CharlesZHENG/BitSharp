using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.ExtensionMethods;
using NLog;
using System;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Numerics;

namespace BitSharp.Core
{
    public class DataEncoder
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();

        public static UInt256 DecodeUInt256(BinaryReader reader)
        {
            return reader.ReadUInt256();
        }

        public static void EncodeUInt256(BinaryWriter writer, UInt256 value)
        {
            writer.WriteUInt256(value);
        }

        public static Block DecodeBlock(BinaryReader reader)
        {
            var header = DecodeBlockHeader(reader);

            var blockTxesCount = reader.ReadVarInt().ToIntChecked();
            var blockTxes = ImmutableArray.CreateBuilder<BlockTx>(blockTxesCount);
            for (var i = 0; i < blockTxesCount; i++)
            {
                var encodedTx = DecodeTransaction(reader);
                var blockTx = new BlockTx(i, encodedTx);
                blockTxes.Add(blockTx);

            }

            return new Block(header, blockTxes.MoveToImmutable());
        }

        public static Block DecodeBlock(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            using (var reader = new BinaryReader(stream))
            {
                return DecodeBlock(reader);
            }
        }

        public static void EncodeBlock(BinaryWriter writer, Block block)
        {
            EncodeBlockHeader(writer, block.Header);
            writer.WriteList(block.BlockTxes, tx => writer.WriteBytes(tx.TxBytes.ToArray()));
        }

        public static byte[] EncodeBlock(Block block)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodeBlock(writer, block);
                return stream.ToArray();
            }
        }

        public static BlockHeader DecodeBlockHeader(BinaryReader reader, UInt256 blockHash = null)
        {
            return new BlockHeader
            (
                version: reader.ReadUInt32(),
                previousBlock: reader.ReadUInt256(),
                merkleRoot: reader.ReadUInt256(),
                time: reader.ReadUInt32(),
                bits: reader.ReadUInt32(),
                nonce: reader.ReadUInt32(),
                hash: blockHash
            );
        }

        public static BlockHeader DecodeBlockHeader(byte[] bytes, UInt256 blockHash = null)
        {
            using (var stream = new MemoryStream(bytes))
            using (var reader = new BinaryReader(stream))
            {
                return DecodeBlockHeader(reader, blockHash);
            }
        }

        public static void EncodeBlockHeader(BinaryWriter writer, BlockHeader blockHeader)
        {
            writer.WriteUInt32(blockHeader.Version);
            writer.WriteUInt256(blockHeader.PreviousBlock);
            writer.WriteUInt256(blockHeader.MerkleRoot);
            writer.WriteUInt32(blockHeader.Time);
            writer.WriteUInt32(blockHeader.Bits);
            writer.WriteUInt32(blockHeader.Nonce);
        }

        public static byte[] EncodeBlockHeader(UInt32 Version, UInt256 PreviousBlock, UInt256 MerkleRoot, UInt32 Time, UInt32 Bits, UInt32 Nonce)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                writer.WriteUInt32(Version);
                writer.WriteUInt256(PreviousBlock);
                writer.WriteUInt256(MerkleRoot);
                writer.WriteUInt32(Time);
                writer.WriteUInt32(Bits);
                writer.WriteUInt32(Nonce);

                return stream.ToArray();
            }
        }

        public static byte[] EncodeBlockHeader(BlockHeader blockHeader)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodeBlockHeader(writer, blockHeader);
                return stream.ToArray();
            }
        }

        public static BigInteger DecodeTotalWork(BinaryReader reader)
        {
            var totalWorkBytesBigEndian = reader.ReadBytes(64);
            var totalWorkBytesLittleEndian = totalWorkBytesBigEndian.Reverse().ToArray();
            return new BigInteger(totalWorkBytesLittleEndian);
        }

        public static BigInteger DecodeTotalWork(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            using (var reader = new BinaryReader(stream))
            {
                return DecodeTotalWork(reader);
            }
        }

        public static void EncodeTotalWork(BinaryWriter writer, BigInteger totalWork)
        {
            if (totalWork < 0)
                throw new ArgumentOutOfRangeException();

            var totalWorkBytesLittleEndian = totalWork.ToByteArray();
            if (totalWorkBytesLittleEndian.Length > 64)
                throw new ArgumentOutOfRangeException();

            var totalWorkBytesLittleEndian64 = new byte[64];
            Buffer.BlockCopy(totalWorkBytesLittleEndian, 0, totalWorkBytesLittleEndian64, 0, totalWorkBytesLittleEndian.Length);

            var totalWorkBytesBigEndian = totalWorkBytesLittleEndian64.Reverse().ToArray();

            writer.WriteBytes(totalWorkBytesBigEndian);
            Debug.Assert(new BigInteger(totalWorkBytesLittleEndian64) == totalWork);
        }

        public static byte[] EncodeTotalWork(BigInteger totalWork)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodeTotalWork(writer, totalWork);
                return stream.ToArray();
            }
        }

        public static ChainedHeader DecodeChainedHeader(BinaryReader reader)
        {
            var blockHash = reader.ReadUInt256();
            return new ChainedHeader
            (
                blockHeader: DecodeBlockHeader(reader, blockHash),
                height: reader.ReadInt32(),
                totalWork: new BigInteger(reader.ReadVarBytes())
            );
        }

        public static ChainedHeader DecodeChainedHeader(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            using (var reader = new BinaryReader(stream))
            {
                return DecodeChainedHeader(reader);
            }
        }

        public static void EncodeChainedHeader(BinaryWriter writer, ChainedHeader chainedHeader)
        {
            writer.WriteUInt256(chainedHeader.Hash);
            EncodeBlockHeader(writer, chainedHeader.BlockHeader);
            writer.WriteInt32(chainedHeader.Height);
            writer.WriteVarBytes(chainedHeader.TotalWork.ToByteArray());
        }

        public static byte[] EncodeChainedHeader(ChainedHeader chainedHeader)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodeChainedHeader(writer, chainedHeader);
                return stream.ToArray();
            }
        }

        public static DecodedTx DecodeTransaction(BinaryReader reader, UInt256 txHash = null)
        {
            var txBytes = new byte[1024];
            var startIndex = 0;

            // read version
            reader.ReadExactly(txBytes, startIndex, 4);
            var version = Bits.ToUInt32(txBytes, startIndex);
            startIndex += 4;

            // read inputs
            var inputCount = reader.ReadVarInt(ref txBytes, ref startIndex).ToIntChecked();
            var inputs = ImmutableArray.CreateBuilder<TxInput>(inputCount);
            for (var i = 0; i < inputCount; i++)
            {
                // read prevTxHash and prevTxOutputIndex
                SizeAtLeast(ref txBytes, startIndex + 36);
                reader.ReadExactly(txBytes, startIndex, 36);
                var prevTxHash = Bits.ToUInt256(txBytes, startIndex);
                var prevTxOutputIndex = Bits.ToUInt32(txBytes, startIndex + 32);
                startIndex += 36;

                // read scriptSignatureLength
                var scriptSignatureLength = reader.ReadVarInt(ref txBytes, ref startIndex).ToIntChecked();

                // read scriptSignature
                SizeAtLeast(ref txBytes, startIndex + scriptSignatureLength);
                reader.ReadExactly(txBytes, startIndex, scriptSignatureLength);
                var scriptSignature = ImmutableArray.Create(txBytes, startIndex, scriptSignatureLength);
                startIndex += scriptSignatureLength;

                // read sequence
                SizeAtLeast(ref txBytes, startIndex + 4);
                reader.ReadExactly(txBytes, startIndex, 4);
                var sequence = Bits.ToUInt32(txBytes, startIndex);
                startIndex += 4;

                var intput = new TxInput(new TxOutputKey(prevTxHash, prevTxOutputIndex), scriptSignature, sequence);
                inputs.Add(intput);
            }

            // read outputs
            var outputCount = reader.ReadVarInt(ref txBytes, ref startIndex).ToIntChecked();
            var outputs = ImmutableArray.CreateBuilder<TxOutput>(outputCount);
            for (var i = 0; i < outputCount; i++)
            {
                // read value
                SizeAtLeast(ref txBytes, startIndex + 8);
                reader.ReadExactly(txBytes, startIndex, 8);
                var value = Bits.ToUInt64(txBytes, startIndex);
                startIndex += 8;

                // read scriptPublicKeyLength
                var scriptPublicKeyLength = reader.ReadVarInt(ref txBytes, ref startIndex).ToIntChecked();

                // read scriptPublicKey
                SizeAtLeast(ref txBytes, startIndex + scriptPublicKeyLength);
                reader.ReadExactly(txBytes, startIndex, scriptPublicKeyLength);
                var scriptPublicKey = ImmutableArray.Create(txBytes, startIndex, scriptPublicKeyLength);
                startIndex += scriptPublicKeyLength;

                var output = new TxOutput(value, scriptPublicKey);
                outputs.Add(output);
            }

            // read lockTime
            SizeAtLeast(ref txBytes, startIndex + 4);
            reader.ReadExactly(txBytes, startIndex, 4);
            var lockTime = Bits.ToUInt32(txBytes, startIndex);
            startIndex += 4;

            // resize raw tx bytes to final size
            Array.Resize(ref txBytes, startIndex);

            txHash = txHash ?? new UInt256(SHA256Static.ComputeDoubleHash(txBytes));

            var tx = new Transaction(version, inputs.MoveToImmutable(), outputs.MoveToImmutable(), lockTime, txHash);
            return new DecodedTx(txBytes.ToImmutableArray(), tx);
        }

        internal static void SizeAtLeast(ref byte[] bytes, int minLength)
        {
            if (bytes.Length < minLength)
                Array.Resize(ref bytes, ((minLength + 1023) / 1024) * 1024);
        }

        public static DecodedTx DecodeTransaction(byte[] bytes, UInt256 txHash = null)
        {
            using (var stream = new MemoryStream(bytes))
            using (var reader = new BinaryReader(stream))
            {
                return DecodeTransaction(reader, txHash);
            }
        }

        public static void EncodeTransaction(BinaryWriter writer, Transaction tx)
        {
            writer.WriteUInt32(tx.Version);
            writer.WriteList(tx.Inputs, input => EncodeTxInput(writer, input));
            writer.WriteList(tx.Outputs, output => EncodeTxOutput(writer, output));
            writer.WriteUInt32(tx.LockTime);
        }

        public static DecodedTx EncodeTransaction(UInt32 Version, ImmutableArray<TxInput> Inputs, ImmutableArray<TxOutput> Outputs, UInt32 LockTime)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                writer.WriteUInt32(Version);
                writer.WriteList(Inputs, input => EncodeTxInput(writer, input));
                writer.WriteList(Outputs, output => EncodeTxOutput(writer, output));
                writer.WriteUInt32(LockTime);

                var txBytes = stream.ToArray();
                var txHash = new UInt256(SHA256Static.ComputeDoubleHash(txBytes));
                var tx = new Transaction(Version, Inputs, Outputs, LockTime, txHash);

                return new DecodedTx(txBytes.ToImmutableArray(), tx);
            }
        }

        public static DecodedTx EncodeTransaction(Transaction tx)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodeTransaction(writer, tx);

                var txBytes = stream.ToArray();

                return new DecodedTx(txBytes.ToImmutableArray(), tx);
            }
        }

        public static TxInput DecodeTxInput(BinaryReader reader)
        {
            return new TxInput
            (
                previousTxOutputKey: new TxOutputKey
                (
                    txHash: reader.ReadUInt256(),
                    txOutputIndex: reader.ReadUInt32()
                ),
                scriptSignature: reader.ReadVarBytes().ToImmutableArray(),
                sequence: reader.ReadUInt32()
            );
        }

        public static TxInput DecodeTxInput(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            using (var reader = new BinaryReader(stream))
            {
                return DecodeTxInput(reader);
            }
        }

        public static void EncodeTxInput(BinaryWriter writer, TxInput txInput)
        {
            writer.WriteUInt256(txInput.PreviousTxOutputKey.TxHash);
            writer.WriteUInt32(txInput.PreviousTxOutputKey.TxOutputIndex);
            writer.WriteVarBytes(txInput.ScriptSignature.ToArray());
            writer.WriteUInt32(txInput.Sequence);
        }

        public static byte[] EncodeTxInput(TxInput txInput)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodeTxInput(writer, txInput);
                return stream.ToArray();
            }
        }

        public static TxOutput DecodeTxOutput(BinaryReader reader)
        {
            return new TxOutput
            (
                value: reader.ReadUInt64(),
                scriptPublicKey: reader.ReadVarBytes().ToImmutableArray()
            );
        }

        public static TxOutput DecodeTxOutput(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            using (var reader = new BinaryReader(stream))
            {
                return DecodeTxOutput(reader);
            }
        }

        public static void EncodeTxOutput(BinaryWriter writer, TxOutput txOutput)
        {
            writer.WriteUInt64(txOutput.Value);
            writer.WriteVarBytes(txOutput.ScriptPublicKey.ToArray());
        }

        public static byte[] EncodeTxOutput(TxOutput txOutput)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodeTxOutput(writer, txOutput);
                return stream.ToArray();
            }
        }

        public static ImmutableArray<TxOutput> DecodeTxOutputList(BinaryReader reader)
        {
            return reader.ReadList(() => DecodeTxOutput(reader));
        }

        public static ImmutableArray<TxOutput> DecodeTxOutputList(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            using (var reader = new BinaryReader(stream))
            {
                return DecodeTxOutputList(reader);
            }
        }

        public static void EncodeTxOutputList(BinaryWriter writer, ImmutableArray<TxOutput> txOutputs)
        {
            writer.WriteList(txOutputs, output => EncodeTxOutput(writer, output));
        }

        public static byte[] EncodeTxOutputList(ImmutableArray<TxOutput> txOutputs)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodeTxOutputList(writer, txOutputs);
                return stream.ToArray();
            }
        }

        public static UnspentTx DecodeUnspentTx(BinaryReader reader)
        {
            var txHash = reader.ReadUInt256();
            var blockIndex = reader.ReadInt32();
            var index = reader.ReadInt32();
            var outputStates = new OutputStates(bytes: reader.ReadVarBytes(), length: reader.ReadInt32());
            var txOutputs = DecodeTxOutputList(reader);

            return new UnspentTx(txHash, blockIndex, index, outputStates, txOutputs);
        }

        public static UnspentTx DecodeUnspentTx(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            using (var reader = new BinaryReader(stream))
            {
                return DecodeUnspentTx(reader);
            }
        }

        public static void EncodeUnspentTx(BinaryWriter writer, UnspentTx unspentTx)
        {
            writer.WriteUInt256(unspentTx.TxHash);
            writer.WriteInt32(unspentTx.BlockIndex);
            writer.WriteInt32(unspentTx.TxIndex);
            writer.WriteVarBytes(unspentTx.OutputStates.ToByteArray());
            writer.WriteInt32(unspentTx.OutputStates.Length);
            EncodeTxOutputList(writer, unspentTx.TxOutputs);
        }

        public static byte[] EncodeUnspentTx(UnspentTx unspentTx)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodeUnspentTx(writer, unspentTx);
                return stream.ToArray();
            }
        }

        public static SpentTx DecodeSpentTx(BinaryReader reader)
        {
            return new SpentTx(
                txHash: reader.ReadUInt256(),
                confirmedBlockIndex: reader.ReadInt32(),
                txIndex: reader.ReadInt32()
            );
        }

        public static SpentTx DecodeSpentTx(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            using (var reader = new BinaryReader(stream))
            {
                return DecodeSpentTx(reader);
            }
        }

        public static void EncodeSpentTx(BinaryWriter writer, SpentTx spentTx)
        {
            writer.WriteUInt256(spentTx.TxHash);
            writer.WriteInt32(spentTx.ConfirmedBlockIndex);
            writer.WriteInt32(spentTx.TxIndex);
        }

        public static byte[] EncodeSpentTx(SpentTx spentTx)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodeSpentTx(writer, spentTx);
                return stream.ToArray();
            }
        }

        public static TxLookupKey DecodeTxLookupKey(BinaryReader reader)
        {
            return new TxLookupKey(
                blockHash: reader.ReadUInt256(),
                txIndex: reader.ReadInt32()
            );
        }

        public static TxLookupKey DecodeTxLookupKey(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            using (var reader = new BinaryReader(stream))
            {
                return DecodeTxLookupKey(reader);
            }
        }

        public static void EncodeTxLookupKey(BinaryWriter writer, TxLookupKey txLookupKey)
        {
            writer.WriteUInt256(txLookupKey.BlockHash);
            writer.WriteInt32(txLookupKey.TxIndex);
        }

        public static byte[] EncodeTxLookupKey(TxLookupKey txLookupKey)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodeTxLookupKey(writer, txLookupKey);
                return stream.ToArray();
            }
        }

        public static UnmintedTx DecodeUnmintedTx(BinaryReader reader)
        {
            var txHash = reader.ReadUInt256();
            var prevTxOutputs = DecodeTxOutputList(reader);

            return new UnmintedTx(txHash, prevTxOutputs);
        }

        public static UnmintedTx DecodeUnmintedTx(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            using (var reader = new BinaryReader(stream))
            {
                return DecodeUnmintedTx(reader);
            }
        }

        public static void EncodeUnmintedTx(BinaryWriter writer, UnmintedTx unmintedTx)
        {
            writer.WriteUInt256(unmintedTx.TxHash);
            EncodeTxOutputList(writer, unmintedTx.PrevTxOutputs);
        }

        public static byte[] EncodeUnmintedTx(UnmintedTx unmintedTx)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodeUnmintedTx(writer, unmintedTx);
                return stream.ToArray();
            }
        }

        public static OutputStates DecodeOutputStates(byte[] bytes)
        {
            var length = BitConverter.ToInt32(bytes, 0);
            var outputStateBytes = new byte[bytes.Length - 4];
            Buffer.BlockCopy(bytes, 4, outputStateBytes, 0, bytes.Length - 4);

            return new OutputStates(outputStateBytes, length);
        }

        public static byte[] EncodeOutputStates(OutputStates outputStates)
        {
            var outputStateBytes = outputStates.ToByteArray();
            var buffer = new byte[4 + outputStateBytes.Length];

            Buffer.BlockCopy(BitConverter.GetBytes(outputStates.Length), 0, buffer, 0, 4);
            Buffer.BlockCopy(outputStateBytes, 0, buffer, 4, outputStateBytes.Length);

            return buffer;
        }

        public static TxOutputKey DecodeTxOutputKey(BinaryReader reader)
        {
            return new TxOutputKey
            (
                txHash: reader.ReadUInt256(),
                txOutputIndex: reader.ReadUInt32()
            );
        }

        public static TxOutputKey DecodeTxOutputKey(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            using (var reader = new BinaryReader(stream))
            {
                return DecodeTxOutputKey(reader);
            }
        }

        public static void EncodeTxOutputKey(BinaryWriter writer, TxOutputKey txOutputKey)
        {
            writer.WriteUInt256(txOutputKey.TxHash);
            writer.WriteUInt32(txOutputKey.TxOutputIndex);
        }

        public static byte[] EncodeTxOutputKey(TxOutputKey txOutputKey)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodeTxOutputKey(writer, txOutputKey);
                return stream.ToArray();
            }
        }

        public static BlockTxNode DecodeBlockTxNode(MemoryStream stream, BinaryReader reader, bool skipTxBytes = false)
        {
            var index = reader.ReadInt32();
            var depth = reader.ReadInt32();
            var hash = reader.ReadUInt256();
            var pruned = reader.ReadBool();

            ImmutableArray<byte>? txBytes;
            if (!skipTxBytes)
            {
                var bytesRemaining = (stream.Length - stream.Position).ToIntChecked();
                txBytes = reader.ReadBytes(bytesRemaining).ToImmutableArray();
            }
            else
                txBytes = null;

            return new BlockTxNode(index, depth, hash, pruned, txBytes);
        }

        public static BlockTxNode DecodeBlockTxNode(byte[] bytes, bool skipTxBytes = false)
        {
            using (var stream = new MemoryStream(bytes))
            using (var reader = new BinaryReader(stream))
            {
                return DecodeBlockTxNode(stream, reader, skipTxBytes);
            }
        }

        public static void EncodeBlockTxNode(BinaryWriter writer, BlockTxNode blockTx)
        {
            writer.WriteInt32(blockTx.Index);
            writer.WriteInt32(blockTx.Depth);
            writer.WriteUInt256(blockTx.Hash);
            writer.WriteBool(blockTx.Pruned);
            if (!blockTx.Pruned)
                writer.WriteBytes(blockTx.TxBytes.ToArray());
        }

        public static byte[] EncodeBlockTxNode(BlockTxNode blockTx)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodeBlockTxNode(writer, blockTx);
                return stream.ToArray();
            }
        }

        public static string DecodeVarString(BinaryReader reader)
        {
            return reader.ReadVarString();
        }

        public static string DecodeVarString(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            using (var reader = new BinaryReader(stream))
            {
                return DecodeVarString(reader);
            }
        }

        public static void EncodeVarString(BinaryWriter writer, string s)
        {
            writer.WriteVarString(s);
        }

        public static byte[] EncodeVarString(string s)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodeVarString(writer, s);
                return stream.ToArray();
            }
        }
    }
}
