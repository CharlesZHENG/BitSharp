using BitSharp.Common;
using BitSharp.Core.Domain;
using BitSharp.Core.ExtensionMethods;
using System;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Text;

namespace BitSharp.Core
{
    public class DataEncoder
    {
        public static UInt256 DecodeUInt256(Stream stream)
        {
            using (var reader = new BinaryReader(stream, Encoding.ASCII, leaveOpen: true))
            {
                return reader.ReadUInt256();
            }
        }

        public static UInt256 DecodeUInt256(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            {
                return DecodeUInt256(stream);
            }
        }

        public static void EncodeUInt256(Stream stream, UInt256 value)
        {
            using (var writer = new BinaryWriter(stream, Encoding.ASCII, leaveOpen: true))
            {
                writer.WriteUInt256(value);
            }
        }

        public static byte[] EncodeUInt256(UInt256 value)
        {
            using (var stream = new MemoryStream())
            {
                EncodeUInt256(stream, value);
                return stream.ToArray();
            }
        }

        public static Block DecodeBlock(Stream stream, UInt256? blockHash = null)
        {
            using (var reader = new BinaryReader(stream, Encoding.ASCII, leaveOpen: true))
            {
                return new Block
                (
                    header: DecodeBlockHeader(stream, blockHash),
                    transactions: reader.ReadList(() => DecodeTransaction(stream))
                );
            }
        }

        public static Block DecodeBlock(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            {
                return DecodeBlock(stream);
            }
        }

        public static void EncodeBlock(Stream stream, Block block)
        {
            using (var writer = new BinaryWriter(stream, Encoding.ASCII, leaveOpen: true))
            {
                EncodeBlockHeader(stream, block.Header);
                writer.WriteList(block.Transactions, tx => EncodeTransaction(stream, tx));
            }
        }

        public static byte[] EncodeBlock(Block block)
        {
            using (var stream = new MemoryStream())
            {
                EncodeBlock(stream, block);
                return stream.ToArray();
            }
        }

        public static BlockHeader DecodeBlockHeader(Stream stream, UInt256? blockHash = null)
        {
            using (var reader = new BinaryReader(stream, Encoding.ASCII, leaveOpen: true))
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
        }

        public static BlockHeader DecodeBlockHeader(byte[] bytes, UInt256? blockHash = null)
        {
            using (var stream = new MemoryStream(bytes))
            {
                return DecodeBlockHeader(stream, blockHash);
            }
        }

        public static void EncodeBlockHeader(Stream stream, BlockHeader blockHeader)
        {
            using (var writer = new BinaryWriter(stream, Encoding.ASCII, leaveOpen: true))
            {
                writer.WriteUInt32(blockHeader.Version);
                writer.WriteUInt256(blockHeader.PreviousBlock);
                writer.WriteUInt256(blockHeader.MerkleRoot);
                writer.WriteUInt32(blockHeader.Time);
                writer.WriteUInt32(blockHeader.Bits);
                writer.WriteUInt32(blockHeader.Nonce);
            }
        }

        public static byte[] EncodeBlockHeader(UInt32 Version, UInt256 PreviousBlock, UInt256 MerkleRoot, UInt32 Time, UInt32 Bits, UInt32 Nonce)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream, Encoding.ASCII, leaveOpen: true))
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
            {
                EncodeBlockHeader(stream, blockHeader);
                return stream.ToArray();
            }
        }

        public static BigInteger DecodeTotalWork(Stream stream)
        {
            using (var reader = new BinaryReader(stream, Encoding.ASCII, leaveOpen: true))
            {
                var totalWorkBytesBigEndian = reader.ReadBytes(64);
                var totalWorkBytesLittleEndian = totalWorkBytesBigEndian.Reverse().ToArray();
                return new BigInteger(totalWorkBytesLittleEndian);
            }
        }

        public static BigInteger DecodeTotalWork(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            {
                return DecodeTotalWork(stream);
            }
        }

        public static void EncodeTotalWork(Stream stream, BigInteger totalWork)
        {
            using (var writer = new BinaryWriter(stream, Encoding.ASCII, leaveOpen: true))
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
        }

        public static byte[] EncodeTotalWork(BigInteger totalWork)
        {
            using (var stream = new MemoryStream())
            {
                EncodeTotalWork(stream, totalWork);
                return stream.ToArray();
            }
        }

        public static ChainedHeader DecodeChainedHeader(Stream stream)
        {
            using (var reader = new BinaryReader(stream, Encoding.ASCII, leaveOpen: true))
            {
                return new ChainedHeader
                (
                    blockHeader: DecodeBlockHeader(stream),
                    height: reader.ReadInt32(),
                    totalWork: new BigInteger(reader.ReadVarBytes())
                );
            }
        }

        public static ChainedHeader DecodeChainedHeader(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            {
                return DecodeChainedHeader(stream);
            }
        }

        public static void EncodeChainedHeader(Stream stream, ChainedHeader chainedHeader)
        {
            using (var writer = new BinaryWriter(stream, Encoding.ASCII, leaveOpen: true))
            {
                EncodeBlockHeader(stream, chainedHeader.BlockHeader);
                writer.WriteInt32(chainedHeader.Height);
                writer.WriteVarBytes(chainedHeader.TotalWork.ToByteArray());
            }
        }

        public static byte[] EncodeChainedHeader(ChainedHeader chainedHeader)
        {
            using (var stream = new MemoryStream())
            {
                EncodeChainedHeader(stream, chainedHeader);
                return stream.ToArray();
            }
        }

        public static Transaction DecodeTransaction(Stream stream, UInt256? txHash = null)
        {
            using (var reader = new BinaryReader(stream, Encoding.ASCII, leaveOpen: true))
            {
                return new Transaction
                (
                    version: reader.ReadUInt32(),
                    inputs: reader.ReadList(() => DecodeTxInput(stream)),
                    outputs: reader.ReadList(() => DecodeTxOutput(stream)),
                    lockTime: reader.ReadUInt32(),
                    hash: txHash
                );
            }
        }

        public static Transaction DecodeTransaction(byte[] bytes, UInt256? txHash = null)
        {
            using (var stream = new MemoryStream(bytes))
            {
                return DecodeTransaction(stream, txHash);
            }
        }

        public static void EncodeTransaction(Stream stream, Transaction tx)
        {
            using (var writer = new BinaryWriter(stream, Encoding.ASCII, leaveOpen: true))
            {
                writer.WriteUInt32(tx.Version);
                writer.WriteList(tx.Inputs, input => EncodeTxInput(stream, input));
                writer.WriteList(tx.Outputs, output => EncodeTxOutput(stream, output));
                writer.WriteUInt32(tx.LockTime);
            }
        }

        public static byte[] EncodeTransaction(UInt32 Version, ImmutableArray<TxInput> Inputs, ImmutableArray<TxOutput> Outputs, UInt32 LockTime)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream, Encoding.ASCII, leaveOpen: true))
            {
                writer.WriteUInt32(Version);
                writer.WriteList(Inputs, input => EncodeTxInput(stream, input));
                writer.WriteList(Outputs, output => EncodeTxOutput(stream, output));
                writer.WriteUInt32(LockTime);

                return stream.ToArray();
            }
        }

        public static byte[] EncodeTransaction(Transaction tx)
        {
            using (var stream = new MemoryStream())
            {
                EncodeTransaction(stream, tx);
                return stream.ToArray();
            }
        }

        public static TxInput DecodeTxInput(Stream stream)
        {
            using (var reader = new BinaryReader(stream, Encoding.ASCII, leaveOpen: true))
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
        }

        public static TxInput DecodeTxInput(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            {
                return DecodeTxInput(stream);
            }
        }

        public static void EncodeTxInput(Stream stream, TxInput txInput)
        {
            using (var writer = new BinaryWriter(stream, Encoding.ASCII, leaveOpen: true))
            {
                writer.WriteUInt256(txInput.PreviousTxOutputKey.TxHash);
                writer.WriteUInt32(txInput.PreviousTxOutputKey.TxOutputIndex);
                writer.WriteVarBytes(txInput.ScriptSignature.ToArray());
                writer.WriteUInt32(txInput.Sequence);
            }
        }

        public static byte[] EncodeTxInput(TxInput txInput)
        {
            using (var stream = new MemoryStream())
            {
                EncodeTxInput(stream, txInput);
                return stream.ToArray();
            }
        }

        public static TxOutput DecodeTxOutput(Stream stream)
        {
            using (var reader = new BinaryReader(stream, Encoding.ASCII, leaveOpen: true))
            {
                return new TxOutput
                (
                    value: reader.ReadUInt64(),
                    scriptPublicKey: reader.ReadVarBytes().ToImmutableArray()
                );
            }
        }

        public static TxOutput DecodeTxOutput(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            {
                return DecodeTxOutput(stream);
            }
        }

        public static void EncodeTxOutput(Stream stream, TxOutput txOutput)
        {
            using (var writer = new BinaryWriter(stream, Encoding.ASCII, leaveOpen: true))
            {
                writer.WriteUInt64(txOutput.Value);
                writer.WriteVarBytes(txOutput.ScriptPublicKey.ToArray());
            }
        }

        public static byte[] EncodeTxOutput(TxOutput txOutput)
        {
            using (var stream = new MemoryStream())
            {
                EncodeTxOutput(stream, txOutput);
                return stream.ToArray();
            }
        }

        public static UnspentTx DecodeUnspentTx(Stream stream)
        {
            using (var reader = new BinaryReader(stream, Encoding.ASCII, leaveOpen: true))
            {
                return new UnspentTx(
                    txHash: reader.ReadUInt256(),
                    blockIndex: reader.ReadInt32(),
                    txIndex: reader.ReadInt32(),
                    outputStates: new OutputStates(
                        bytes: reader.ReadVarBytes(),
                        length: reader.ReadInt32())
                );
            }
        }

        public static UnspentTx DecodeUnspentTx(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            {
                return DecodeUnspentTx(stream);
            }
        }

        public static void EncodeUnspentTx(Stream stream, UnspentTx unspentTx)
        {
            using (var writer = new BinaryWriter(stream, Encoding.ASCII, leaveOpen: true))
            {
                writer.WriteUInt256(unspentTx.TxHash);
                writer.WriteInt32(unspentTx.BlockIndex);
                writer.WriteInt32(unspentTx.TxIndex);
                writer.WriteVarBytes(unspentTx.OutputStates.ToByteArray());
                writer.WriteInt32(unspentTx.OutputStates.Length);
            }
        }

        public static byte[] EncodeUnspentTx(UnspentTx unspentTx)
        {
            using (var stream = new MemoryStream())
            {
                EncodeUnspentTx(stream, unspentTx);
                return stream.ToArray();
            }
        }

        public static SpentTx DecodeSpentTx(Stream stream)
        {
            using (var reader = new BinaryReader(stream, Encoding.ASCII, leaveOpen: true))
            {
                return new SpentTx(
                    txHash: reader.ReadUInt256(),
                    confirmedBlockIndex: reader.ReadInt32(),
                    txIndex: reader.ReadInt32(),
                    outputCount: reader.ReadInt32(),
                    spentBlockIndex: reader.ReadInt32()
                );
            }
        }

        public static SpentTx DecodeSpentTx(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            {
                return DecodeSpentTx(stream);
            }
        }

        public static void EncodeSpentTx(Stream stream, SpentTx spentTx)
        {
            using (var writer = new BinaryWriter(stream, Encoding.ASCII, leaveOpen: true))
            {
                writer.WriteUInt256(spentTx.TxHash);
                writer.WriteInt32(spentTx.ConfirmedBlockIndex);
                writer.WriteInt32(spentTx.TxIndex);
                writer.WriteInt32(spentTx.OutputCount);
                writer.WriteInt32(spentTx.SpentBlockIndex);
            }
        }

        public static byte[] EncodeSpentTx(SpentTx spentTx)
        {
            using (var stream = new MemoryStream())
            {
                EncodeSpentTx(stream, spentTx);
                return stream.ToArray();
            }
        }

        public static BlockTxKey DecodeBlockTxKey(Stream stream)
        {
            using (var reader = new BinaryReader(stream, Encoding.ASCII, leaveOpen: true))
            {
                return new BlockTxKey(
                    blockHash: reader.ReadUInt256(),
                    txIndex: reader.ReadInt32()
                );
            }
        }

        public static BlockTxKey DecodeBlockTxKey(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            {
                return DecodeBlockTxKey(stream);
            }
        }

        public static void EncodeBlockTxKey(Stream stream, BlockTxKey blockTxKey)
        {
            using (var writer = new BinaryWriter(stream, Encoding.ASCII, leaveOpen: true))
            {
                writer.WriteUInt256(blockTxKey.BlockHash);
                writer.WriteInt32(blockTxKey.TxIndex);
            }
        }

        public static byte[] EncodeBlockTxKey(BlockTxKey blockTxKey)
        {
            using (var stream = new MemoryStream())
            {
                EncodeBlockTxKey(stream, blockTxKey);
                return stream.ToArray();
            }
        }

        public static UnmintedTx DecodeUnmintedTx(Stream stream)
        {
            using (var reader = new BinaryReader(stream, Encoding.ASCII, leaveOpen: true))
            {
                return new UnmintedTx(
                    txHash: reader.ReadUInt256(),
                    prevOutputTxKeys: reader.ReadList(() => DecodeBlockTxKey(stream))
                );
            }
        }

        public static UnmintedTx DecodeUnmintedTx(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            {
                return DecodeUnmintedTx(stream);
            }
        }

        public static void EncodeUnmintedTx(Stream stream, UnmintedTx unmintedTx)
        {
            using (var writer = new BinaryWriter(stream, Encoding.ASCII, leaveOpen: true))
            {
                writer.WriteUInt256(unmintedTx.TxHash);
                writer.WriteList(unmintedTx.PrevOutputTxKeys, x => EncodeBlockTxKey(stream, x));
            }
        }

        public static byte[] EncodeUnmintedTx(UnmintedTx unmintedTx)
        {
            using (var stream = new MemoryStream())
            {
                EncodeUnmintedTx(stream, unmintedTx);
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

        public static TxOutputKey DecodeTxOutputKey(Stream stream)
        {
            using (var reader = new BinaryReader(stream, Encoding.ASCII, leaveOpen: true))
            {
                return new TxOutputKey
                (
                    txHash: reader.ReadUInt256(),
                    txOutputIndex: reader.ReadUInt32()
                );
            }
        }

        public static TxOutputKey DecodeTxOutputKey(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            {
                return DecodeTxOutputKey(stream);
            }
        }

        public static void EncodeTxOutputKey(Stream stream, TxOutputKey txOutputKey)
        {
            using (var writer = new BinaryWriter(stream, Encoding.ASCII, leaveOpen: true))
            {
                writer.WriteUInt256(txOutputKey.TxHash);
                writer.WriteUInt32(txOutputKey.TxOutputIndex);
            }
        }

        public static byte[] EncodeTxOutputKey(TxOutputKey txOutputKey)
        {
            using (var stream = new MemoryStream())
            {
                EncodeTxOutputKey(stream, txOutputKey);
                return stream.ToArray();
            }
        }

        public static string DecodeVarString(Stream stream)
        {
            using (var reader = new BinaryReader(stream, Encoding.ASCII, leaveOpen: true))
            {
                return reader.ReadVarString();
            }
        }

        public static string DecodeVarString(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            {
                return DecodeVarString(stream);
            }
        }

        public static void EncodeVarString(Stream stream, string s)
        {
            using (var writer = new BinaryWriter(stream, Encoding.ASCII, leaveOpen: true))
            {
                writer.WriteVarString(s);
            }
        }

        public static byte[] EncodeVarString(string s)
        {
            using (var stream = new MemoryStream())
            {
                EncodeVarString(stream, s);
                return stream.ToArray();
            }
        }
    }
}
