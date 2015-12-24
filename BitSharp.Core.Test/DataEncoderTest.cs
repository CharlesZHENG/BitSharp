using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Linq;

namespace BitSharp.Core.Test
{
    [TestClass]
    public partial class DataEncoderTest
    {
        [TestMethod]
        public void TestWireEncodeBlockHeader()
        {
            var actual = DataEncoder.EncodeBlockHeader(BLOCK_HEADER_1);
            CollectionAssert.AreEqual(BLOCK_HEADER_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireDecodeBlockHeader()
        {
            var actual = DataEncoder.EncodeBlockHeader(DataEncoder.DecodeBlockHeader(BLOCK_HEADER_1_BYTES.ToArray()));
            CollectionAssert.AreEqual(BLOCK_HEADER_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireEncodeBlock()
        {
            var actual = DataEncoder.EncodeBlock(BLOCK_1);
            CollectionAssert.AreEqual(BLOCK_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireDecodeBlock()
        {
            var actual = DataEncoder.EncodeBlock(DataEncoder.DecodeBlock(BLOCK_1_BYTES.ToArray()));
            CollectionAssert.AreEqual(BLOCK_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireEncodeTransactionIn()
        {
            var actual = DataEncoder.EncodeTxInput(TRANSACTION_INPUT_1);
            CollectionAssert.AreEqual(TRANSACTION_INPUT_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireDecodeTransactionIn()
        {
            var actual = DataEncoder.EncodeTxInput(DataEncoder.DecodeTxInput(TRANSACTION_INPUT_1_BYTES.ToArray()));
            CollectionAssert.AreEqual(TRANSACTION_INPUT_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireEncodeTransactionOut()
        {
            var actual = DataEncoder.EncodeTxOutput(TRANSACTION_OUTPUT_1);
            CollectionAssert.AreEqual(TRANSACTION_OUTPUT_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireDecodeTransactionOut()
        {
            var actual = DataEncoder.EncodeTxOutput(DataEncoder.DecodeTxOutput(TRANSACTION_OUTPUT_1_BYTES.ToArray()));
            CollectionAssert.AreEqual(TRANSACTION_OUTPUT_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireEncodeTransaction()
        {
            var actual = DataEncoder.EncodeTransaction(TRANSACTION_1);
            CollectionAssert.AreEqual(TRANSACTION_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireDecodeTransaction()
        {
            var actual = DataEncoder.EncodeTransaction(DataEncoder.DecodeTransaction(TRANSACTION_1_BYTES.ToArray()).Decode());
            CollectionAssert.AreEqual(TRANSACTION_1_BYTES.ToList(), actual.ToList());
        }
    }
}
