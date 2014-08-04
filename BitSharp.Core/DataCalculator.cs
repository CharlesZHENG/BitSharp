﻿using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace BitSharp.Core
{
    public static class DataCalculator
    {
        public static UInt256 CalculateBlockHash(BlockHeader blockHeader)
        {
            var sha256 = new SHA256Managed();
            return new UInt256(sha256.ComputeDoubleHash(DataEncoder.EncodeBlockHeader(blockHeader)));
        }

        public static UInt256 CalculateBlockHash(UInt32 Version, UInt256 PreviousBlock, UInt256 MerkleRoot, UInt32 Time, UInt32 Bits, UInt32 Nonce)
        {
            var sha256 = new SHA256Managed();
            return new UInt256(sha256.ComputeDoubleHash(DataEncoder.EncodeBlockHeader(Version, PreviousBlock, MerkleRoot, Time, Bits, Nonce)));
        }

        public static UInt256 CalculateTransactionHash(Transaction tx)
        {
            var sha256 = new SHA256Managed();
            return new UInt256(sha256.ComputeDoubleHash(DataEncoder.EncodeTransaction(tx)));
        }

        public static UInt256 CalculateTransactionHash(UInt32 Version, ImmutableArray<TxInput> Inputs, ImmutableArray<TxOutput> Outputs, UInt32 LockTime)
        {
            var sha256 = new SHA256Managed();
            return new UInt256(sha256.ComputeDoubleHash(DataEncoder.EncodeTransaction(Version, Inputs, Outputs, LockTime)));
        }

        //TDOO name...
        private static readonly BigInteger Max256BitTarget = BigInteger.Pow(2, 256);
        public static BigInteger CalculateWork(BlockHeader blockHeader)
        {
            try
            {
                return Max256BitTarget / (BigInteger)BitsToTarget(blockHeader.Bits);
            }
            catch (Exception)
            {
                Debug.WriteLine("Corrupt block header bits: {0}, block {1}".Format2(blockHeader.Bits.ToString("X"), blockHeader.Hash.ToHexNumberString()));
                return -1;
            }
        }

        public static UInt256 BitsToTarget(UInt32 bits)
        {
            // last three bytes store the multiplicand
            var multiplicand = (BigInteger)bits % 0x1000000;
            if (multiplicand > 0x7fffff)
                throw new ArgumentOutOfRangeException("bits");

            // first byte stores the value to be used in the power
            var powerPart = (int)(bits >> 24);
            var multiplier = BigInteger.Pow(2, 8 * (powerPart - 3));

            return new UInt256(multiplicand * multiplier);
        }

        public static UInt32 TargetToBits(UInt256 target)
        {
            // to get the powerPart: take the log in base 2, round up to 8 to respect byte boundaries, and then remove 24 to represent 3 bytes of precision
            var log = Math.Ceiling(UInt256.Log(target, 2) / 8) * 8 - 24;
            var powerPart = (byte)(log / 8 + 3);

            // determine the multiplier based on the powerPart
            var multiplier = BigInteger.Pow(2, 8 * (powerPart - 3));

            // to get multiplicand: divide the target by the multiplier
            //TODO
            var multiplicandBytes = ((BigInteger)target / (BigInteger)multiplier).ToByteArray();
            Debug.Assert(multiplicandBytes.Length == 3 || multiplicandBytes.Length == 4);

            // this happens when multipicand would be greater than 0x7fffff
            // TODO need a better explanation comment
            if (multiplicandBytes.Last() == 0)
            {
                multiplicandBytes = multiplicandBytes.Skip(1).ToArray();
                powerPart++;
            }

            // construct the bits representing the powerPart and multiplicand
            var bits = Bits.ToUInt32(multiplicandBytes.Concat(powerPart).ToArray());
            return bits;
        }
    }
}
