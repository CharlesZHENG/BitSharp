using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Immutable;

namespace BitSharp.Core.Test
{
    [TestClass]
    public class DataCalculatorTest
    {
        [TestMethod]
        public void TestBitsToTarget()
        {
            var bits1 = 0x1b0404cbU;
            var expected1 = UInt256.ParseHex("404cb000000000000000000000000000000000000000000000000");
            var actual1 = DataCalculator.BitsToTarget(bits1);
            Assert.AreEqual(expected1, actual1);

            // difficulty: 1
            var bits2 = 0x1d00ffffU;
            var expected2 = UInt256.ParseHex("ffff0000000000000000000000000000000000000000000000000000");
            var actual2 = DataCalculator.BitsToTarget(bits2);
            Assert.AreEqual(expected2, actual2);
        }

        [TestMethod]
        public void TestTargetToBits()
        {
            var target1 = UInt256.ParseHex("404cb000000000000000000000000000000000000000000000000");
            var expected1 = 0x1b0404cbU;
            var actual1 = DataCalculator.TargetToBits(target1);

            Assert.AreEqual(expected1, actual1);

            // difficulty: 1
            var target2 = UInt256.ParseHex("ffff0000000000000000000000000000000000000000000000000000");
            var expected2 = 0x1d00ffffU;
            var actual2 = DataCalculator.TargetToBits(target2);

            Assert.AreEqual(expected2, actual2);

            var target3 = UInt256.ParseHex("7fff0000000000000000000000000000000000000000000000000000");
            var expected3 = 0x1c7fff00U;
            var actual3 = DataCalculator.TargetToBits(target3);

            Assert.AreEqual(expected3, actual3);
        }

        [TestMethod]
        public void TestCalculateBlockHash()
        {
            var expectedHash = UInt256.ParseHex("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f");
            var blockHeader = BlockHeader.Create
            (
                version: 1,
                previousBlock: UInt256.Zero,
                merkleRoot: UInt256.ParseHex("4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"),
                time: DateTimeOffset.FromUnixTimeSeconds(1231006505),
                bits: 0x1D00FFFF,
                nonce: 2083236893
            );

            Assert.AreEqual(expectedHash, DataCalculator.CalculateBlockHash(blockHeader));
            Assert.AreEqual(expectedHash, DataCalculator.CalculateBlockHash(blockHeader.Version, blockHeader.PreviousBlock, blockHeader.MerkleRoot, blockHeader.Time, blockHeader.Bits, blockHeader.Nonce));
        }

        [TestMethod]
        public void TestCalculateTransactionHash()
        {
            var expectedHash = UInt256.ParseHex("4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b");
            var tx = Transaction.Create
            (
                version: 1,
                inputs: ImmutableArray.Create(
                    new TxInput
                    (
                        prevTxHash: UInt256.Zero,
                        prevTxOutputIndex: 4294967295,
                        scriptSignature: "04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73".HexToByteArray().ToImmutableArray(),
                        sequence: 4294967295
                    )),
                outputs: ImmutableArray.Create(
                    new TxOutput
                    (
                        value: (UInt64)(50L * 100.MILLION()),
                        scriptPublicKey: "4104678afdb0fe5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb649f6bc3f4cef38c4f35504e51ec112de5c384df7ba0b8d578a4c702b6bf11d5fac".HexToByteArray().ToImmutableArray()
                    )),
                lockTime: 0
            ).Transaction;

            Assert.AreEqual(expectedHash, tx.Hash);
        }
    }
}
