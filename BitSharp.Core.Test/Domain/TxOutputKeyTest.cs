using BitSharp.Core.Domain;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace BitSharp.Core.Test.Domain
{
    [TestClass]
    public class TxOutputKeyTest
    {
        [TestMethod]
        public void TestTxOutputKeyEquality()
        {
            var randomTxOutputKey = RandomData.RandomTxOutputKey();

            var sameTxOutputKey = new TxOutputKey
            (
                txHash: randomTxOutputKey.TxHash,
                txOutputIndex: randomTxOutputKey.TxOutputIndex
            );

            var differentTxOutputKeyTxHash = new TxOutputKey
            (
                txHash: ~randomTxOutputKey.TxHash,
                txOutputIndex: randomTxOutputKey.TxOutputIndex
            );

            var differentTxOutputKeyTxOutputIndex = new TxOutputKey
            (
                txHash: randomTxOutputKey.TxHash,
                txOutputIndex: ~randomTxOutputKey.TxOutputIndex
            );

            Assert.IsTrue(randomTxOutputKey.Equals(sameTxOutputKey));
            Assert.IsTrue(randomTxOutputKey == sameTxOutputKey);
            Assert.IsFalse(randomTxOutputKey != sameTxOutputKey);

            Assert.IsFalse(randomTxOutputKey.Equals(differentTxOutputKeyTxHash));
            Assert.IsFalse(randomTxOutputKey == differentTxOutputKeyTxHash);
            Assert.IsTrue(randomTxOutputKey != differentTxOutputKeyTxHash);

            Assert.IsFalse(randomTxOutputKey.Equals(differentTxOutputKeyTxOutputIndex));
            Assert.IsFalse(randomTxOutputKey == differentTxOutputKeyTxOutputIndex);
            Assert.IsTrue(randomTxOutputKey != differentTxOutputKeyTxOutputIndex);
        }
    }
}
