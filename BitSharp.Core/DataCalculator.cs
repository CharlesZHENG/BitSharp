using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using System;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Numerics;

namespace BitSharp.Core
{
    public static class DataCalculator
    {
        private static readonly BigInteger _2Pow256 = BigInteger.Pow(2, 256);

        public static UInt256 CalculateBlockHash(BlockHeader blockHeader)
        {
            return new UInt256(SHA256Static.ComputeDoubleHash(DataEncoder.EncodeBlockHeader(blockHeader)));
        }

        public static UInt256 CalculateBlockHash(UInt32 Version, UInt256 PreviousBlock, UInt256 MerkleRoot, DateTimeOffset Time, UInt32 Bits, UInt32 Nonce)
        {
            return new UInt256(SHA256Static.ComputeDoubleHash(DataEncoder.EncodeBlockHeader(Version, PreviousBlock, MerkleRoot, Time, Bits, Nonce)));
        }

        public static UInt256 CalculateWork(BlockHeader blockHeader)
        {
            bool negative, overflow;
            var target = FromCompact(blockHeader.Bits, out negative, out overflow);

            if (negative || overflow || target == UInt256.Zero)
                return UInt256.Zero;

            return new UInt256(_2Pow256 / (target.ToBigInteger() + 1));
        }

        public static UInt256 FromCompact(uint compact)
        {
            bool negative, overflow;
            return FromCompact(compact, out negative, out overflow);
        }

        public static UInt256 FromCompact(uint compact, out bool negative, out bool overflow)
        {
            var size = (int)(compact >> 24);
            var word = compact & 0x007fffff;

            UInt256 value;
            if (size <= 3)
            {
                word >>= 8 * (3 - size);
                value = (UInt256)word;
            }
            else
            {
                value = (UInt256)word;
                value <<= 8 * (size - 3);
            }

            negative = word != 0 && (compact & 0x00800000) != 0;
            overflow = word != 0 && ((size > 34) ||
                                     (word > 0xff && size > 33) ||
                                     (word > 0xffff && size > 32));

            return value;
        }

        public static uint ToCompact(UInt256 value, bool negative = false)
        {
            var size = (HighBit(value) + 7) / 8;
            var compact = 0U;

            if (size <= 3)
            {
                compact = (uint)(value.Part4 << 8 * (3 - size));
            }
            else
            {
                value >>= 8 * (size - 3);
                compact = (uint)(value.Part4);
            }

            // The 0x00800000 bit denotes the sign.
            // Thus, if it is already set, divide the mantissa by 256 and increase the exponent.
            if ((compact & 0x00800000) != 0)
            {
                compact >>= 8;
                size++;
            }

            Debug.Assert((compact & ~0x007fffff) == 0);
            Debug.Assert(size < 256);

            compact |= (uint)(size << 24);
            if (negative && (compact & 0x007fffff) != 0)
                compact |= 0x00800000;

            return compact;
        }

        private static int HighBit(UInt256 value)
        {
            var parts = value.Parts;
            for (var pos = parts.Length - 1; pos >= 0; pos--)
            {
                var part = parts[pos];
                if (part != 0)
                {
                    for (var bits = 63; bits > 0; bits--)
                    {
                        if ((part & 1UL << bits) != 0)
                            return 64 * pos + bits + 1;
                    }
                    return 64 * pos + 1;
                }
            }

            return 0;
        }
    }
}
