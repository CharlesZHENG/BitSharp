using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Common.Test;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks.Dataflow;

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
            var loadingTxes = new BufferBlock<LoadingTx>();
            loadingTxes.Post(loadingTx);
            loadingTxes.Complete();

            // begin transaction loading
            var txLoader = TxLoader.LoadTxes(coreStorageMock.Object, loadingTxes);

            // verify the loaded transaction
            var loadedTxesBuffer = new BufferBlock<LoadedTx>();
            txLoader.LinkTo(loadedTxesBuffer, new DataflowLinkOptions { PropagateCompletion = true });
            txLoader.Completion.Wait();

            IList<LoadedTx> actualLoadedTxes;
            Assert.IsTrue(loadedTxesBuffer.TryReceiveAll(out actualLoadedTxes));

            var actualLoadedTx = actualLoadedTxes.Single();

            Assert.AreEqual(loadingTx.TxIndex, actualLoadedTx.TxIndex);
            Assert.AreEqual(loadingTx.Transaction, actualLoadedTx.Transaction);
            CollectionAssert.AreEqual(prevTxes, actualLoadedTx.InputTxes);
        }

        /// <summary>
        /// Verify that an exception in the loading txes source is properly propagated.
        /// </summary>
        [TestMethod]
        public void TestExceptionInLoadingTxes()
        {
            var expectedException = new Exception();

            var coreStorage = Mock.Of<ICoreStorage>();

            var loadingTxes = new BufferBlock<LoadingTx>();
            ((IDataflowBlock)loadingTxes).Fault(expectedException);

            var loadedTxes = TxLoader.LoadTxes(coreStorage, loadingTxes);

            using (var loadedTxesQueue = loadedTxes.LinkToQueue())
            {
                Exception actualEx;
                AssertMethods.AssertAggregateThrows<Exception>(() =>
                    loadedTxesQueue.GetConsumingEnumerable().ToList(), out actualEx);
                Assert.AreSame(expectedException, actualEx);
            }
        }

        /// <summary>
        /// Verify that an exception thrown while loading a tx input is properly propagated.
        /// </summary>
        [TestMethod]
        public void TestExceptionInLoadTxInput()
        {
            var expectedException = new Exception();

            var coreStorage = new Mock<ICoreStorage>();

            var chainedHeader = RandomData.RandomChainedHeader();
            var tx = RandomData.RandomTransaction(new RandomDataOptions { TxInputCount = 1 });
            var txLookupKey = new TxLookupKey(UInt256.Zero, 0);
            var loadingTx = new LoadingTx(1, tx, chainedHeader, ImmutableArray.Create(txLookupKey));

            var loadingTxes = new BufferBlock<LoadingTx>();
            loadingTxes.Post(loadingTx);
            loadingTxes.Complete();

            // throw expected exception when the input transaction is looked up
            Transaction outputTx = null;
            coreStorage.Setup(x => x.TryGetTransaction(txLookupKey.BlockHash, txLookupKey.TxIndex, out outputTx)).Throws(expectedException);

            var loadedTxes = TxLoader.LoadTxes(coreStorage.Object, loadingTxes);

            using (var loadedTxesQueue = loadedTxes.LinkToQueue())
            {
                Exception actualEx;
                AssertMethods.AssertAggregateThrows<Exception>(() =>
                    loadedTxesQueue.GetConsumingEnumerable().ToList(), out actualEx);
                Assert.AreSame(expectedException, actualEx);
            }
        }
    }
}
