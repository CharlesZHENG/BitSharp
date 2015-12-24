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

        public static UInt256 DecodeUInt256(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            using (var reader = new BinaryReader(stream))
            {
                return DecodeUInt256(reader);
            }
        }

        public static void EncodeUInt256(BinaryWriter writer, UInt256 value)
        {
            writer.WriteUInt256(value);
        }

        public static byte[] EncodeUInt256(UInt256 value)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodeUInt256(writer, value);
                return stream.ToArray();
            }
        }

        public static Block DecodeBlock(BinaryReader reader, UInt256 blockHash = null)
        {
            return new Block
            (
                header: DecodeBlockHeader(reader, blockHash),
                transactions: reader.ReadList(() => DecodeTransaction(reader))
            );
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
            writer.WriteList(block.Transactions, tx => EncodeTransaction(writer, tx));
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

        public static Transaction DecodeTransaction(BinaryReader reader, UInt256 txHash = null)
        {
            return new Transaction
            (
                version: reader.ReadUInt32(),
                inputs: reader.ReadList(() => DecodeTxInput(reader)),
                outputs: reader.ReadList(() => DecodeTxOutput(reader)),
                lockTime: reader.ReadUInt32(),
                hash: txHash
            );
        }

        public static Transaction DecodeTransaction(byte[] bytes, UInt256 txHash = null)
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

        public static byte[] EncodeTransaction(UInt32 Version, ImmutableArray<TxInput> Inputs, ImmutableArray<TxOutput> Outputs, UInt32 LockTime)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                writer.WriteUInt32(Version);
                writer.WriteList(Inputs, input => EncodeTxInput(writer, input));
                writer.WriteList(Outputs, output => EncodeTxOutput(writer, output));
                writer.WriteUInt32(LockTime);

                return stream.ToArray();
            }
        }

        public static byte[] EncodeTransaction(Transaction tx)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodeTransaction(writer, tx);
                return stream.ToArray();
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
            var prevOutputTxKeys = reader.ReadList(() => DecodeTxLookupKey(reader));

            return new UnmintedTx(txHash, prevOutputTxKeys);
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
            writer.WriteList(unmintedTx.PrevOutputTxKeys, x => EncodeTxLookupKey(writer, x));
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

        public static BlockTx DecodeBlockTx(MemoryStream stream, BinaryReader reader, bool skipTx = false)
        {
            var index = reader.ReadInt32();
            var depth = reader.ReadInt32();
            var hash = reader.ReadUInt256();
            var pruned = reader.ReadBool();

            if (!pruned && !skipTx)
            {
                var bytesRemaining = (stream.Length - stream.Position).ToIntChecked();
                var txBytes = reader.ReadBytes(bytesRemaining).ToImmutableArray();

                return new BlockTx(index, depth, hash, pruned, txBytes);
            }
            else
                return new BlockTx(index, depth, hash, pruned, (ImmutableArray<byte>?)null);
        }

        public static BlockTx DecodeBlockTx(byte[] bytes, bool skipTx = false)
        {
            using (var stream = new MemoryStream(bytes))
            using (var reader = new BinaryReader(stream))
            {
                return DecodeBlockTx(stream, reader, skipTx);
            }
        }

        public static void EncodeBlockTx(BinaryWriter writer, BlockTx blockTx)
        {
            writer.WriteInt32(blockTx.Index);
            writer.WriteInt32(blockTx.Depth);
            writer.WriteUInt256(blockTx.Hash);
            writer.WriteBool(blockTx.Pruned);
            if (!blockTx.Pruned)
                writer.WriteBytes(blockTx.TxBytes.Value.ToArray());
        }

        public static byte[] EncodeBlockTx(BlockTx blockTx)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodeBlockTx(writer, blockTx);
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
