using BitSharp.Common;
using System;

namespace BitSharp.LevelDb
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

        public static int DecodeInt32(byte[] value, int offset)
        {
            var bytes = new byte[4];
            Buffer.BlockCopy(value, offset, bytes, 0, 4);
            Array.Reverse(bytes);

            return Bits.ToInt32(bytes);
        }

        public static void DecodeBlockHashTxIndex(byte[] bytes, out UInt256 blockHash, out int txIndex)
        {
            blockHash = UInt256.FromByteArrayBE(bytes);
            txIndex = DecodeInt32(bytes, 32);
        }

        public static byte[] EncodeBlockHashTxIndex(UInt256 blockHash, int txIndex)
        {
            var blockHashTxIndexBytes = new byte[36];
            blockHash.ToByteArrayBE(blockHashTxIndexBytes);
            Buffer.BlockCopy(EncodeInt32(txIndex), 0, blockHashTxIndexBytes, 32, 4);

            return blockHashTxIndexBytes;
        }
    }
}
