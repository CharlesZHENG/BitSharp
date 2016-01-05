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

namespace BitSharp.Core
{
    public class DataEncoder
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();

        public static void EncodeUInt256(BinaryWriter writer, UInt256 value)
        {
            writer.WriteUInt256(value);
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

        public static void EncodeChainedHeader(BinaryWriter writer, ChainedHeader chainedHeader)
        {
            writer.WriteUInt256(chainedHeader.Hash);
            EncodeBlockHeader(writer, chainedHeader.BlockHeader);
            writer.WriteInt32(chainedHeader.Height);
            writer.WriteVarBytes(chainedHeader.TotalWork.ToByteArray());
            writer.WriteInt64(chainedHeader.DateSeen.Ticks);
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

        public static void EncodeTxInput(BinaryWriter writer, TxInput txInput)
        {
            writer.WriteUInt256(txInput.PrevTxOutputKey.TxHash);
            writer.WriteUInt32(txInput.PrevTxOutputKey.TxOutputIndex);
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

        public static void EncodePrevTxOutput(BinaryWriter writer, PrevTxOutput txOutput)
        {
            writer.WriteUInt64(txOutput.Value);
            writer.WriteVarBytes(txOutput.ScriptPublicKey.ToArray());
            writer.WriteInt32(txOutput.BlockHeight);
            writer.WriteInt32(txOutput.TxIndex);
            writer.WriteUInt32(txOutput.TxVersion);
            writer.WriteBool(txOutput.IsCoinbase);
        }

        public static byte[] EncodePrevTxOutput(PrevTxOutput txOutput)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodePrevTxOutput(writer, txOutput);
                return stream.ToArray();
            }
        }

        public static void EncodePrevTxOutputList(BinaryWriter writer, ImmutableArray<PrevTxOutput> txOutputs)
        {
            writer.WriteList(txOutputs, output => EncodePrevTxOutput(writer, output));
        }

        public static byte[] EncodePrevTxOutputList(ImmutableArray<PrevTxOutput> txOutputs)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodePrevTxOutputList(writer, txOutputs);
                return stream.ToArray();
            }
        }

        public static void EncodeUnspentTx(BinaryWriter writer, UnspentTx unspentTx)
        {
            writer.WriteUInt256(unspentTx.TxHash);
            writer.WriteInt32(unspentTx.BlockIndex);
            writer.WriteInt32(unspentTx.TxIndex);
            writer.WriteUInt32(unspentTx.TxVersion);
            writer.WriteBool(unspentTx.IsCoinbase);
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

        public static void EncodeUnmintedTx(BinaryWriter writer, UnmintedTx unmintedTx)
        {
            writer.WriteUInt256(unmintedTx.TxHash);
            EncodePrevTxOutputList(writer, unmintedTx.PrevTxOutputs);
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

        public static void EncodeUnmintedTxList(BinaryWriter writer, IImmutableList<UnmintedTx> unmintedTxes)
        {
            writer.WriteList(unmintedTxes, unmintedTx => EncodeUnmintedTx(writer, unmintedTx));
        }

        public static byte[] EncodeUnmintedTxList(IImmutableList<UnmintedTx> unmintedTxes)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodeUnmintedTxList(writer, unmintedTxes);
                return stream.ToArray();
            }
        }

        public static byte[] EncodeOutputStates(OutputStates outputStates)
        {
            var outputStateBytes = outputStates.ToByteArray();
            var buffer = new byte[4 + outputStateBytes.Length];

            Buffer.BlockCopy(BitConverter.GetBytes(outputStates.Length), 0, buffer, 0, 4);
            Buffer.BlockCopy(outputStateBytes, 0, buffer, 4, outputStateBytes.Length);

            return buffer;
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

        public static void EncodeBlockSpentTxes(BinaryWriter writer, BlockSpentTxes blockSpentTxes)
        {
            writer.WriteList(blockSpentTxes, spentTx => EncodeSpentTx(writer, spentTx));
        }

        public static byte[] EncodeBlockSpentTxes(BlockSpentTxes blockSpentTxes)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodeBlockSpentTxes(writer, blockSpentTxes);
                return stream.ToArray();
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

        public static int VarIntSize(UInt64 value)
        {
            if (value < 0xFD)
            {
                return 1;
            }
            else if (value <= 0xFFFF)
            {
                return 2;
            }
            else if (value <= 0xFFFFFFFF)
            {
                return 4;
            }
            else
            {
                return 8;
            }
        }
    }
}
