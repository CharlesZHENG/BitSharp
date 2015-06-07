using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using System;

namespace BitSharp.Lmdb
{
    public static class DbEncoder
    {
        public static byte[] EncodeUInt256(UInt256 value)
        {
            return value.ToByteArrayBE();
        }

        public static UInt256 DecodeUInt256(byte[] value)
        {
            return UInt256.FromByteArrayBE(value);
        }

        public static byte[] EncodeInt32(int value)
        {
            var bytes = Bits.GetBytes(value);
            Array.Reverse(bytes);
            return bytes;
        }

        public static int DecodeInt32(byte[] value)
        {
            var bytes = new byte[4];
            Buffer.BlockCopy(value, 0, bytes, 0, 4);
            Array.Reverse(bytes);

            return Bits.ToInt32(bytes);
        }

        public static void DecodeBlockHashTxIndex(byte[] bytes, out UInt256 blockHash, out int txIndex)
        {
            var blockHashBytes = new byte[32];
            var txIndexBytes = new byte[4];

            Buffer.BlockCopy(bytes, 0, blockHashBytes, 0, 32);
            Buffer.BlockCopy(bytes, 32, txIndexBytes, 0, 4);

            blockHash = DbEncoder.DecodeUInt256(blockHashBytes);
            txIndex = DbEncoder.DecodeInt32(txIndexBytes);
        }

        public static byte[] EncodeBlockHashTxIndex(UInt256 blockHash, int txIndex)
        {
            var blockHashTxIndexBytes = new byte[36];
            Buffer.BlockCopy(DbEncoder.EncodeUInt256(blockHash), 0, blockHashTxIndexBytes, 0, 32);
            Buffer.BlockCopy(DbEncoder.EncodeInt32(txIndex), 0, blockHashTxIndexBytes, 32, 4);

            return blockHashTxIndexBytes;
        }

        public static TxOutputKey DecodeTxOutputKey(byte[] bytes)
        {
            var txHashBytes = new byte[32];
            var outputIndexBytes = new byte[4];

            Buffer.BlockCopy(bytes, 0, txHashBytes, 0, 32);
            Buffer.BlockCopy(bytes, 32, outputIndexBytes, 0, 4);

            return new TxOutputKey(DbEncoder.DecodeUInt256(txHashBytes), DbEncoder.DecodeInt32(outputIndexBytes).ToUIntChecked());
        }

        public static byte[] EncodeTxOutputKey(TxOutputKey txOutputKey)
        {
            var txOutputKeyBytes = new byte[36];
            Buffer.BlockCopy(DbEncoder.EncodeUInt256(txOutputKey.TxHash), 0, txOutputKeyBytes, 0, 32);
            Buffer.BlockCopy(DbEncoder.EncodeInt32(txOutputKey.TxOutputIndex.ToIntChecked()), 0, txOutputKeyBytes, 32, 4);

            return txOutputKeyBytes;
        }
    }
}
