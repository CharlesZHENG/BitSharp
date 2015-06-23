using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Core.Test.Builders
{
    [TestClass]
    public class TxLoaderTest
    {
        /// <summary>
        /// Verify loading a single transaction successfully.
        /// </summary>
        [TestMethod]
        public void TestReadOneLoadingTx()
        {
            var coreStorageMock = new Mock<ICoreStorage>();
            using (var txLoader = new TxLoader("", coreStorageMock.Object, threadCount: 1))
            using (var loadingTxesReader = new ParallelReader<LoadingTx>(""))
            {
                // create a fake transaction with 4 inputs
                var prevTxCount = 4;
                var txIndex = 1;
                var tx = RandomData.RandomTransaction(new RandomDataOptions { TxInputCount = prevTxCount, TxOutputCount = 1 });
                var chainedHeader = RandomData.RandomChainedHeader();

                // create a loading tx with the 4 inputs referencing block hash 0
                var prevOutputTxKeys = ImmutableArray.CreateRange(
                    Enumerable.Range(0, prevTxCount).Select(x => new TxLookupKey(UInt256.Zero, x)));
                var loadingTx = new LoadingTx(txIndex, tx, chainedHeader, prevOutputTxKeys);

                // create previous transactions for the 4 inputs
                var prevTxes = new Transaction[prevTxCount];
                for (var i = 0; i < prevTxCount; i++)
                {
                    // create previous transaction, ensuring its hash matches what the input expects
                    var prevTx = RandomData.RandomTransaction().With(Hash: tx.Inputs[i].PreviousTxOutputKey.TxHash);
                    prevTxes[i] = prevTx;

                    // mock retrieval of the previous transaction
                    coreStorageMock.Setup(coreStorage => coreStorage.TryGetTransaction(UInt256.Zero, i, out prevTx)).Returns(true);
                }

                // begin queuing transactions to load
                using (loadingTxesReader.ReadAsync(new[] { loadingTx }).WaitOnDispose())
                // begin transaction loading
                using (txLoader.LoadTxes(loadingTxesReader).WaitOnDispose())
                {

                    // verify the loaded transaction
                    var actualLoadedTx = txLoader.GetConsumingEnumerable().Single();

                    Assert.AreEqual(loadingTx.TxIndex, actualLoadedTx.TxIndex);
                    Assert.AreEqual(loadingTx.Transaction, actualLoadedTx.Transaction);
                    CollectionAssert.AreEqual(prevTxes, actualLoadedTx.InputTxes);
                }
            }
        }
    }
}
