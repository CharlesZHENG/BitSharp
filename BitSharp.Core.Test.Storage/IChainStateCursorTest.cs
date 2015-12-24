using BitSharp.Common;
using BitSharp.Common.Test;
using BitSharp.Core.Domain;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;

namespace BitSharp.Core.Test.Storage
{
    [TestClass]
    public class IChainStateCursorTest : StorageProviderTest
    {
        [TestMethod]
        public void TestTransactionIsolation()
        {
            RunTest(TestTransactionIsolation);
        }

        [TestMethod]
        public void TestInTransaction()
        {
            RunTest(TestInTransaction);
        }

        [TestMethod]
        public void TestBeginTransaction()
        {
            RunTest(TestBeginTransaction);
        }

        [TestMethod]
        public void TestCommitTransaction()
        {
            RunTest(TestCommitTransaction);
        }

        [TestMethod]
        public void TestRollbackTransaction()
        {
            RunTest(TestRollbackTransaction);
        }

        [TestMethod]
        public void TestChainTip()
        {
            RunTest(TestChainTip);
        }

        [TestMethod]
        public void TestUnspentTxCount()
        {
            RunTest(TestUnspentTxCount);
        }

        [TestMethod]
        public void TestContainsHeader()
        {
            RunTest(TestContainsHeader);
        }

        [TestMethod]
        public void TestTryAddGetRemoveHeader()
        {
            RunTest(TestTryAddGetRemoveHeader);
        }

        [TestMethod]
        public void TestContainsUnspentTx()
        {
            RunTest(TestContainsUnspentTx);
        }

        [TestMethod]
        public void TestTryAddGetRemoveUnspentTx()
        {
            RunTest(TestTryAddGetRemoveUnspentTx);
        }

        [TestMethod]
        public void TestTryUpdateUnspentTx()
        {
            RunTest(TestTryUpdateUnspentTx);
        }

        [TestMethod]
        public void TestReadUnspentTransactions()
        {
            RunTest(TestReadUnspentTransactions);
        }

        [TestMethod]
        public void TestContainsBlockSpentTxes()
        {
            RunTest(TestContainsBlockSpentTxes);
        }

        [TestMethod]
        public void TestTryAddGetRemoveBlockSpentTxes()
        {
            RunTest(TestTryAddGetRemoveBlockSpentTxes);
        }

        [TestMethod]
        public void TestContainsBlockUnmintedTxes()
        {
            RunTest(TestContainsBlockUnmintedTxes);
        }

        [TestMethod]
        public void TestTryAddGetRemoveBlockUnmintedTxes()
        {
            RunTest(TestTryAddGetRemoveBlockUnmintedTxes);
        }

        [TestMethod]
        public void TestFlush()
        {
            RunTest(TestFlush);
        }

        [TestMethod]
        public void TestDefragment()
        {
            RunTest(TestDefragment);
        }

        [TestMethod]
        public void TestOperationOutsideTransaction()
        {
            RunTest(TestOperationOutsideTransaction);
        }

        [TestMethod]
        public void TestWriteOperationInReadonlyTransaction()
        {
            RunTest(TestWriteOperationInReadonlyTransaction);
        }

        [TestMethod]
        public void TestAccessAcrossThreads()
        {
            RunTest(TestAccessAcrossThreads);
        }

        private void TestTransactionIsolation(ITestStorageProvider provider)
        {
            var fakeHeaders = new FakeHeaders();
            var chainedHeader0 = fakeHeaders.GenesisChained();

            // open two chain state cursors
            using (var storageManager = provider.OpenStorageManager())
            using (var txBeganEvent = new ManualResetEventSlim())
            using (var headerAddedEvent = new ManualResetEventSlim())
            using (var doCommitEvent = new ManualResetEventSlim())
            using (var committedEvent = new ManualResetEventSlim())
            {
                var thread1 = new Thread(() =>
                {
                    using (var handle1 = storageManager.OpenChainStateCursor())
                    {
                        var chainStateCursor1 = handle1.Item;

                        // open transactions on cusor 1
                        chainStateCursor1.BeginTransaction();

                        // add a header on cursor 1
                        chainStateCursor1.ChainTip = chainedHeader0;

                        // alert header has been added
                        headerAddedEvent.Set();

                        doCommitEvent.Wait();

                        // commit transaction on cursor 1
                        chainStateCursor1.CommitTransaction();

                        // alert transaction has been committed
                        committedEvent.Set();
                    }
                });
                thread1.Start();

                // wait for header to be added on cursor 1
                headerAddedEvent.Wait();

                using (var handle2 = storageManager.OpenChainStateCursor())
                {
                    var chainStateCursor2 = handle2.Item;

                    // open transactions on cusor 2
                    chainStateCursor2.BeginTransaction(readOnly: true);

                    // verify empty chain
                    Assert.IsNull(chainStateCursor2.ChainTip);

                    doCommitEvent.Set();
                    committedEvent.Wait();

                    // verify empty chain
                    Assert.IsNull(chainStateCursor2.ChainTip);

                    chainStateCursor2.CommitTransaction();
                    chainStateCursor2.BeginTransaction();

                    // verify cursor 2 now sees the new header
                    Assert.AreEqual(chainedHeader0, chainStateCursor2.ChainTip);

                    chainStateCursor2.RollbackTransaction();
                }

                thread1.Join();
            }
        }

        private void TestInTransaction(ITestStorageProvider provider)
        {
            using (var storageManager = provider.OpenStorageManager())
            using (var handle = storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                // verify initial InTransaction=false
                Assert.IsFalse(chainStateCursor.InTransaction);

                // begin transaction and verify InTransaction=true
                chainStateCursor.BeginTransaction();
                Assert.IsTrue(chainStateCursor.InTransaction);

                // rollback transaction and verify InTransaction=false
                chainStateCursor.RollbackTransaction();
                Assert.IsFalse(chainStateCursor.InTransaction);

                // begin transaction and verify InTransaction=true
                chainStateCursor.BeginTransaction();
                Assert.IsTrue(chainStateCursor.InTransaction);

                // commit transaction and verify InTransaction=false
                chainStateCursor.CommitTransaction();
                Assert.IsFalse(chainStateCursor.InTransaction);
            }
        }

        private void TestBeginTransaction(ITestStorageProvider provider)
        {
            Assert.Inconclusive("TODO");
        }

        private void TestCommitTransaction(ITestStorageProvider provider)
        {
            var fakeHeaders = new FakeHeaders();
            var chainedHeader0 = fakeHeaders.GenesisChained();
            var unspentTx = new UnspentTx(txHash: UInt256.Zero, blockIndex: 0, txIndex: 0, outputStates: new OutputStates(1, OutputState.Spent), txOutputs: ImmutableArray<TxOutput>.Empty);
            var spentTxes = BlockSpentTxes.CreateRange(new[] { unspentTx.ToSpentTx() });

            using (var storageManager = provider.OpenStorageManager())
            using (var handle = storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                // begin transaction
                chainStateCursor.BeginTransaction();

                // add data
                chainStateCursor.ChainTip = chainedHeader0;
                chainStateCursor.TryAddUnspentTx(unspentTx);
                chainStateCursor.UnspentTxCount++;
                chainStateCursor.TryAddBlockSpentTxes(0, spentTxes);

                // verify data
                Assert.AreEqual(chainedHeader0, chainStateCursor.ChainTip);
                Assert.AreEqual(1, chainStateCursor.UnspentTxCount);

                UnspentTx actualUnspentTx;
                Assert.IsTrue(chainStateCursor.TryGetUnspentTx(unspentTx.TxHash, out actualUnspentTx));
                Assert.AreEqual(unspentTx, actualUnspentTx);

                BlockSpentTxes actualSpentTxes;
                Assert.IsTrue(chainStateCursor.TryGetBlockSpentTxes(0, out actualSpentTxes));
                CollectionAssert.AreEqual(spentTxes.ToList(), actualSpentTxes.ToList());

                // commit transaction
                chainStateCursor.CommitTransaction();

                chainStateCursor.BeginTransaction();

                // verify data
                Assert.AreEqual(chainedHeader0, chainStateCursor.ChainTip);
                Assert.AreEqual(1, chainStateCursor.UnspentTxCount);

                Assert.IsTrue(chainStateCursor.TryGetUnspentTx(unspentTx.TxHash, out actualUnspentTx));
                Assert.AreEqual(unspentTx, actualUnspentTx);

                Assert.IsTrue(chainStateCursor.TryGetBlockSpentTxes(0, out actualSpentTxes));
                CollectionAssert.AreEqual(spentTxes.ToList(), actualSpentTxes.ToList());

                chainStateCursor.RollbackTransaction();
            }
        }

        private void TestRollbackTransaction(ITestStorageProvider provider)
        {
            var fakeHeaders = new FakeHeaders();
            var chainedHeader0 = fakeHeaders.GenesisChained();
            var chainedHeader1 = fakeHeaders.NextChained();
            var chainedHeader2 = fakeHeaders.NextChained();

            var unspentTx = new UnspentTx(txHash: UInt256.Zero, blockIndex: 0, txIndex: 0, outputStates: new OutputStates(1, OutputState.Spent), txOutputs: ImmutableArray<TxOutput>.Empty);
            var spentTxes = BlockSpentTxes.CreateRange(new[] { unspentTx.ToSpentTx() });

            using (var storageManager = provider.OpenStorageManager())
            using (var handle = storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                // begin transaction
                chainStateCursor.BeginTransaction();

                // add header 0
                chainStateCursor.ChainTip = chainedHeader0;

                // verify chain
                Assert.AreEqual(chainedHeader0, chainStateCursor.ChainTip);

                // add unspent tx
                chainStateCursor.TryAddUnspentTx(unspentTx);
                chainStateCursor.UnspentTxCount++;

                // verify unspent tx
                Assert.IsTrue(chainStateCursor.ContainsUnspentTx(unspentTx.TxHash));
                Assert.AreEqual(1, chainStateCursor.UnspentTxCount);

                // add spent txes
                chainStateCursor.TryAddBlockSpentTxes(0, spentTxes);

                // verify spent txes
                BlockSpentTxes actualSpentTxes;
                Assert.IsTrue(chainStateCursor.TryGetBlockSpentTxes(0, out actualSpentTxes));
                CollectionAssert.AreEqual(spentTxes.ToList(), actualSpentTxes.ToList());

                // rollback transaction
                chainStateCursor.RollbackTransaction();

                chainStateCursor.BeginTransaction();

                // verify chain
                Assert.IsNull(chainStateCursor.ChainTip);

                // verify unspent tx
                Assert.IsFalse(chainStateCursor.ContainsUnspentTx(unspentTx.TxHash));
                Assert.AreEqual(0, chainStateCursor.UnspentTxCount);

                // verify spent txes
                Assert.IsFalse(chainStateCursor.TryGetBlockSpentTxes(0, out actualSpentTxes));

                chainStateCursor.RollbackTransaction();
            }
        }

        private void TestChainTip(ITestStorageProvider provider)
        {
            var fakeHeaders = new FakeHeaders();
            var chainedHeader0 = fakeHeaders.GenesisChained();
            var chainedHeader1 = fakeHeaders.NextChained();

            using (var storageManager = provider.OpenStorageManager())
            using (var handle = storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                // begin transaction
                chainStateCursor.BeginTransaction();

                // verify initial tip
                Assert.IsNull(chainStateCursor.ChainTip);

                // set tip
                chainStateCursor.ChainTip = chainedHeader0;

                // verify tip
                Assert.AreEqual(chainedHeader0, chainStateCursor.ChainTip);

                // set tip
                chainStateCursor.ChainTip = chainedHeader1;

                // verify tip
                Assert.AreEqual(chainedHeader1, chainStateCursor.ChainTip);
            }
        }

        private void TestUnspentTxCount(ITestStorageProvider provider)
        {
            var unspentTx0 = new UnspentTx(txHash: (UInt256)0, blockIndex: 0, txIndex: 0, outputStates: new OutputStates(1, OutputState.Unspent), txOutputs: ImmutableArray<TxOutput>.Empty);
            var unspentTx1 = new UnspentTx(txHash: (UInt256)1, blockIndex: 0, txIndex: 0, outputStates: new OutputStates(1, OutputState.Unspent), txOutputs: ImmutableArray<TxOutput>.Empty);
            var unspentTx2 = new UnspentTx(txHash: (UInt256)2, blockIndex: 0, txIndex: 0, outputStates: new OutputStates(1, OutputState.Unspent), txOutputs: ImmutableArray<TxOutput>.Empty);

            using (var storageManager = provider.OpenStorageManager())
            using (var handle = storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                // begin transaction
                chainStateCursor.BeginTransaction();

                // verify initial count
                Assert.AreEqual(0, chainStateCursor.UnspentTxCount);

                // increment count
                chainStateCursor.UnspentTxCount++;

                // verify count
                Assert.AreEqual(1, chainStateCursor.UnspentTxCount);

                // set count
                chainStateCursor.UnspentTxCount = 10;

                // verify count
                Assert.AreEqual(10, chainStateCursor.UnspentTxCount);
            }
        }

        private void TestContainsHeader(ITestStorageProvider provider)
        {
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.GenesisChained();
            var header1 = fakeHeaders.NextChained();

            using (var storageManager = provider.OpenStorageManager())
            using (var handle = storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                // begin transaction
                chainStateCursor.BeginTransaction();

                // verify presence
                Assert.IsFalse(chainStateCursor.ContainsHeader(header0.Hash));
                Assert.IsFalse(chainStateCursor.ContainsHeader(header1.Hash));

                // add header 0
                chainStateCursor.TryAddHeader(header0);

                // verify presence
                Assert.IsTrue(chainStateCursor.ContainsHeader(header0.Hash));
                Assert.IsFalse(chainStateCursor.ContainsHeader(header1.Hash));

                // add header 1
                chainStateCursor.TryAddHeader(header1);

                // verify presence
                Assert.IsTrue(chainStateCursor.ContainsHeader(header0.Hash));
                Assert.IsTrue(chainStateCursor.ContainsHeader(header1.Hash));

                // remove header 1
                chainStateCursor.TryRemoveHeader(header1.Hash);

                // verify presence
                Assert.IsTrue(chainStateCursor.ContainsHeader(header0.Hash));
                Assert.IsFalse(chainStateCursor.ContainsHeader(header1.Hash));

                // remove header 0
                chainStateCursor.TryRemoveHeader(header0.Hash);

                // verify presence
                Assert.IsFalse(chainStateCursor.ContainsHeader(header0.Hash));
                Assert.IsFalse(chainStateCursor.ContainsHeader(header1.Hash));
            }
        }

        private void TestTryAddGetRemoveHeader(ITestStorageProvider provider)
        {
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.GenesisChained();
            var header1 = fakeHeaders.NextChained();

            using (var storageManager = provider.OpenStorageManager())
            using (var handle = storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                // begin transaction
                chainStateCursor.BeginTransaction();

                // verify initial empty state
                ChainedHeader actualHeader0, actualHeader1;
                Assert.IsFalse(chainStateCursor.TryGetHeader(header0.Hash, out actualHeader0));
                Assert.IsFalse(chainStateCursor.TryGetHeader(header0.Hash, out actualHeader1));

                // add header 0
                Assert.IsTrue(chainStateCursor.TryAddHeader(header0));

                // verify unspent txes
                Assert.IsTrue(chainStateCursor.TryGetHeader(header0.Hash, out actualHeader0));
                Assert.AreEqual(header0, actualHeader0);
                Assert.IsFalse(chainStateCursor.TryGetHeader(header1.Hash, out actualHeader1));

                // add header 1
                Assert.IsTrue(chainStateCursor.TryAddHeader(header1));

                // verify unspent txes
                Assert.IsTrue(chainStateCursor.TryGetHeader(header0.Hash, out actualHeader0));
                Assert.AreEqual(header0, actualHeader0);
                Assert.IsTrue(chainStateCursor.TryGetHeader(header1.Hash, out actualHeader1));
                Assert.AreEqual(header1, actualHeader1);

                // remove header 1
                Assert.IsTrue(chainStateCursor.TryRemoveHeader(header1.Hash));

                // verify unspent txes
                Assert.IsTrue(chainStateCursor.TryGetHeader(header0.Hash, out actualHeader0));
                Assert.AreEqual(header0, actualHeader0);
                Assert.IsFalse(chainStateCursor.TryGetHeader(header1.Hash, out actualHeader1));

                // remove header 0
                Assert.IsTrue(chainStateCursor.TryRemoveHeader(header0.Hash));

                // verify unspent txes
                Assert.IsFalse(chainStateCursor.TryGetHeader(header0.Hash, out actualHeader0));
                Assert.IsFalse(chainStateCursor.TryGetHeader(header1.Hash, out actualHeader1));
            }
        }

        private void TestContainsUnspentTx(ITestStorageProvider provider)
        {
            var unspentTx0 = new UnspentTx(txHash: (UInt256)0, blockIndex: 0, txIndex: 0, outputStates: new OutputStates(1, OutputState.Unspent), txOutputs: ImmutableArray<TxOutput>.Empty);
            var unspentTx1 = new UnspentTx(txHash: (UInt256)1, blockIndex: 0, txIndex: 0, outputStates: new OutputStates(1, OutputState.Unspent), txOutputs: ImmutableArray<TxOutput>.Empty);

            using (var storageManager = provider.OpenStorageManager())
            using (var handle = storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                // begin transaction
                chainStateCursor.BeginTransaction();

                // verify presence
                Assert.IsFalse(chainStateCursor.ContainsUnspentTx(unspentTx0.TxHash));
                Assert.IsFalse(chainStateCursor.ContainsUnspentTx(unspentTx1.TxHash));

                // add unspent tx 0
                chainStateCursor.TryAddUnspentTx(unspentTx0);

                // verify presence
                Assert.IsTrue(chainStateCursor.ContainsUnspentTx(unspentTx0.TxHash));
                Assert.IsFalse(chainStateCursor.ContainsUnspentTx(unspentTx1.TxHash));

                // add unspent tx 1
                chainStateCursor.TryAddUnspentTx(unspentTx1);

                // verify presence
                Assert.IsTrue(chainStateCursor.ContainsUnspentTx(unspentTx0.TxHash));
                Assert.IsTrue(chainStateCursor.ContainsUnspentTx(unspentTx1.TxHash));

                // remove unspent tx 1
                chainStateCursor.TryRemoveUnspentTx(unspentTx1.TxHash);

                // verify presence
                Assert.IsTrue(chainStateCursor.ContainsUnspentTx(unspentTx0.TxHash));
                Assert.IsFalse(chainStateCursor.ContainsUnspentTx(unspentTx1.TxHash));

                // remove unspent tx 0
                chainStateCursor.TryRemoveUnspentTx(unspentTx0.TxHash);

                // verify presence
                Assert.IsFalse(chainStateCursor.ContainsUnspentTx(unspentTx0.TxHash));
                Assert.IsFalse(chainStateCursor.ContainsUnspentTx(unspentTx1.TxHash));
            }
        }

        private void TestTryAddGetRemoveUnspentTx(ITestStorageProvider provider)
        {
            var unspentTx0 = new UnspentTx(txHash: (UInt256)0, blockIndex: 0, txIndex: 0, outputStates: new OutputStates(1, OutputState.Unspent), txOutputs: ImmutableArray<TxOutput>.Empty);
            var unspentTx1 = new UnspentTx(txHash: (UInt256)1, blockIndex: 0, txIndex: 0, outputStates: new OutputStates(1, OutputState.Unspent), txOutputs: ImmutableArray<TxOutput>.Empty);

            using (var storageManager = provider.OpenStorageManager())
            using (var handle = storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                // begin transaction
                chainStateCursor.BeginTransaction();

                // verify initial empty state
                UnspentTx actualUnspentTx0, actualUnspentTx1;
                Assert.IsFalse(chainStateCursor.TryGetUnspentTx(unspentTx0.TxHash, out actualUnspentTx0));
                Assert.IsFalse(chainStateCursor.TryGetUnspentTx(unspentTx1.TxHash, out actualUnspentTx1));

                // add unspent tx 0
                Assert.IsTrue(chainStateCursor.TryAddUnspentTx(unspentTx0));

                // verify unspent txes
                Assert.IsTrue(chainStateCursor.TryGetUnspentTx(unspentTx0.TxHash, out actualUnspentTx0));
                Assert.AreEqual(unspentTx0, actualUnspentTx0);
                Assert.IsFalse(chainStateCursor.TryGetUnspentTx(unspentTx1.TxHash, out actualUnspentTx1));

                // add unspent tx 1
                Assert.IsTrue(chainStateCursor.TryAddUnspentTx(unspentTx1));

                // verify unspent txes
                Assert.IsTrue(chainStateCursor.TryGetUnspentTx(unspentTx0.TxHash, out actualUnspentTx0));
                Assert.AreEqual(unspentTx0, actualUnspentTx0);
                Assert.IsTrue(chainStateCursor.TryGetUnspentTx(unspentTx1.TxHash, out actualUnspentTx1));
                Assert.AreEqual(unspentTx1, actualUnspentTx1);

                // remove unspent tx 1
                Assert.IsTrue(chainStateCursor.TryRemoveUnspentTx(unspentTx1.TxHash));

                // verify unspent txes
                Assert.IsTrue(chainStateCursor.TryGetUnspentTx(unspentTx0.TxHash, out actualUnspentTx0));
                Assert.AreEqual(unspentTx0, actualUnspentTx0);
                Assert.IsFalse(chainStateCursor.TryGetUnspentTx(unspentTx1.TxHash, out actualUnspentTx1));

                // remove unspent tx 0
                Assert.IsTrue(chainStateCursor.TryRemoveUnspentTx(unspentTx0.TxHash));

                // verify unspent txes
                Assert.IsFalse(chainStateCursor.TryGetUnspentTx(unspentTx0.TxHash, out actualUnspentTx0));
                Assert.IsFalse(chainStateCursor.TryGetUnspentTx(unspentTx1.TxHash, out actualUnspentTx1));
            }
        }

        private void TestTryUpdateUnspentTx(ITestStorageProvider provider)
        {
            var unspentTx = new UnspentTx(txHash: UInt256.Zero, blockIndex: 0, txIndex: 0, outputStates: new OutputStates(1, OutputState.Unspent), txOutputs: ImmutableArray<TxOutput>.Empty);
            var unspentTxUpdated = unspentTx.SetOutputState(0, OutputState.Spent);
            Assert.AreNotEqual(unspentTx, unspentTxUpdated);

            using (var storageManager = provider.OpenStorageManager())
            using (var handle = storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                // begin transaction
                chainStateCursor.BeginTransaction();

                // verify can't update missing unspent tx
                Assert.IsFalse(chainStateCursor.TryUpdateUnspentTx(unspentTx));

                // add unspent tx
                Assert.IsTrue(chainStateCursor.TryAddUnspentTx(unspentTx));

                // verify unspent tx
                UnspentTx actualUnspentTx;
                Assert.IsTrue(chainStateCursor.TryGetUnspentTx(unspentTx.TxHash, out actualUnspentTx));
                Assert.AreEqual(unspentTx, actualUnspentTx);

                // update unspent tx
                Assert.IsTrue(chainStateCursor.TryUpdateUnspentTx(unspentTxUpdated));

                // verify updated unspent tx
                Assert.IsTrue(chainStateCursor.TryGetUnspentTx(unspentTx.TxHash, out actualUnspentTx));
                Assert.AreEqual(unspentTxUpdated, actualUnspentTx);

                // remove unspent tx
                Assert.IsTrue(chainStateCursor.TryRemoveUnspentTx(unspentTx.TxHash));

                // verify can't update missing unspent tx
                Assert.IsFalse(chainStateCursor.TryUpdateUnspentTx(unspentTx));
            }
        }

        public void TestReadUnspentTransactions(ITestStorageProvider provider)
        {
            var unspentTx0 = new UnspentTx(txHash: (UInt256)0, blockIndex: 0, txIndex: 0, outputStates: new OutputStates(1, OutputState.Unspent), txOutputs: ImmutableArray<TxOutput>.Empty);
            var unspentTx1 = new UnspentTx(txHash: (UInt256)1, blockIndex: 0, txIndex: 0, outputStates: new OutputStates(1, OutputState.Unspent), txOutputs: ImmutableArray<TxOutput>.Empty);
            var unspentTx2 = new UnspentTx(txHash: (UInt256)2, blockIndex: 0, txIndex: 0, outputStates: new OutputStates(1, OutputState.Unspent), txOutputs: ImmutableArray<TxOutput>.Empty);

            using (var storageManager = provider.OpenStorageManager())
            using (var handle = storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                // begin transaction
                chainStateCursor.BeginTransaction();

                // verify initial empty state
                Assert.AreEqual(0, chainStateCursor.ReadUnspentTransactions().Count());

                // add unspent tx 0
                Assert.IsTrue(chainStateCursor.TryAddUnspentTx(unspentTx0));

                // verify unspent txes
                CollectionAssert.AreEquivalent(new[] { unspentTx0 }, chainStateCursor.ReadUnspentTransactions().ToList());

                // add unspent tx 1
                Assert.IsTrue(chainStateCursor.TryAddUnspentTx(unspentTx1));

                // verify unspent txes
                CollectionAssert.AreEquivalent(new[] { unspentTx0, unspentTx1 }, chainStateCursor.ReadUnspentTransactions().ToList());

                // add unspent tx 2
                Assert.IsTrue(chainStateCursor.TryAddUnspentTx(unspentTx2));

                // verify unspent txes
                CollectionAssert.AreEquivalent(new[] { unspentTx0, unspentTx1, unspentTx2 }, chainStateCursor.ReadUnspentTransactions().ToList());

                // remove unspent tx 2
                Assert.IsTrue(chainStateCursor.TryRemoveUnspentTx(unspentTx2.TxHash));

                // verify unspent txes
                CollectionAssert.AreEquivalent(new[] { unspentTx0, unspentTx1 }, chainStateCursor.ReadUnspentTransactions().ToList());

                // remove unspent tx 1
                Assert.IsTrue(chainStateCursor.TryRemoveUnspentTx(unspentTx1.TxHash));

                // verify unspent txes
                CollectionAssert.AreEquivalent(new[] { unspentTx0 }, chainStateCursor.ReadUnspentTransactions().ToList());

                // remove unspent tx 0
                Assert.IsTrue(chainStateCursor.TryRemoveUnspentTx(unspentTx0.TxHash));

                // verify unspent txes
                Assert.AreEqual(0, chainStateCursor.ReadUnspentTransactions().Count());
            }
        }

        public void TestContainsBlockSpentTxes(ITestStorageProvider provider)
        {
            var spentTxes0 = BlockSpentTxes.CreateRange(new[] {
                new SpentTx((UInt256)0, 0, 0),
                new SpentTx((UInt256)1, 0, 1),
                new SpentTx((UInt256)2, 0, 2)});
            var spentTxes1 = BlockSpentTxes.CreateRange(new[] {
                new SpentTx((UInt256)100, 0, 100),
                new SpentTx((UInt256)101, 0, 101)});

            using (var storageManager = provider.OpenStorageManager())
            using (var handle = storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                // begin transaction
                chainStateCursor.BeginTransaction();

                // verify presence
                Assert.IsFalse(chainStateCursor.ContainsBlockSpentTxes(0));
                Assert.IsFalse(chainStateCursor.ContainsBlockSpentTxes(1));

                // add spent txes 0
                chainStateCursor.TryAddBlockSpentTxes(0, spentTxes0);

                // verify presence
                Assert.IsTrue(chainStateCursor.ContainsBlockSpentTxes(0));
                Assert.IsFalse(chainStateCursor.ContainsBlockSpentTxes(1));

                // add unspent tx 1
                chainStateCursor.TryAddBlockSpentTxes(1, spentTxes1);

                // verify presence
                Assert.IsTrue(chainStateCursor.ContainsBlockSpentTxes(0));
                Assert.IsTrue(chainStateCursor.ContainsBlockSpentTxes(1));

                // remove unspent tx 1
                chainStateCursor.TryRemoveBlockSpentTxes(1);

                // verify presence
                Assert.IsTrue(chainStateCursor.ContainsBlockSpentTxes(0));
                Assert.IsFalse(chainStateCursor.ContainsBlockSpentTxes(1));

                // remove unspent tx 0
                chainStateCursor.TryRemoveBlockSpentTxes(0);

                // verify presence
                Assert.IsFalse(chainStateCursor.ContainsBlockSpentTxes(0));
                Assert.IsFalse(chainStateCursor.ContainsBlockSpentTxes(1));
            }
        }

        public void TestTryAddGetRemoveBlockSpentTxes(ITestStorageProvider provider)
        {
            var spentTxes0 = BlockSpentTxes.CreateRange(new[] {
                new SpentTx((UInt256)0, 0, 0),
                new SpentTx((UInt256)1, 0, 1),
                new SpentTx((UInt256)2, 0, 2)});
            var spentTxes1 = BlockSpentTxes.CreateRange(new[] {
                new SpentTx((UInt256)100, 0, 100),
                new SpentTx((UInt256)101, 0, 101)});

            using (var storageManager = provider.OpenStorageManager())
            using (var handle = storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                // begin transaction
                chainStateCursor.BeginTransaction();

                // verify initial empty state
                BlockSpentTxes actualSpentTxes0, actualSpentTxes1;
                Assert.IsFalse(chainStateCursor.TryGetBlockSpentTxes(0, out actualSpentTxes0));
                Assert.IsFalse(chainStateCursor.TryGetBlockSpentTxes(1, out actualSpentTxes1));

                // add spent txes 0
                Assert.IsTrue(chainStateCursor.TryAddBlockSpentTxes(0, spentTxes0));

                // verify spent txes
                Assert.IsTrue(chainStateCursor.TryGetBlockSpentTxes(0, out actualSpentTxes0));
                CollectionAssert.AreEqual(spentTxes0.ToList(), actualSpentTxes0.ToList());
                Assert.IsFalse(chainStateCursor.TryGetBlockSpentTxes(1, out actualSpentTxes1));

                // add spent txes 1
                Assert.IsTrue(chainStateCursor.TryAddBlockSpentTxes(1, spentTxes1));

                // verify spent txes
                Assert.IsTrue(chainStateCursor.TryGetBlockSpentTxes(0, out actualSpentTxes0));
                CollectionAssert.AreEqual(spentTxes0.ToList(), actualSpentTxes0.ToList());
                Assert.IsTrue(chainStateCursor.TryGetBlockSpentTxes(1, out actualSpentTxes1));
                CollectionAssert.AreEqual(spentTxes1.ToList(), actualSpentTxes1.ToList());

                // remove spent txes 1
                Assert.IsTrue(chainStateCursor.TryRemoveBlockSpentTxes(1));

                // verify spent txes
                Assert.IsTrue(chainStateCursor.TryGetBlockSpentTxes(0, out actualSpentTxes0));
                CollectionAssert.AreEqual(spentTxes0.ToList(), actualSpentTxes0.ToList());
                Assert.IsFalse(chainStateCursor.TryGetBlockSpentTxes(1, out actualSpentTxes1));

                // remove spent txes 0
                Assert.IsTrue(chainStateCursor.TryRemoveBlockSpentTxes(0));

                // verify spent txes
                Assert.IsFalse(chainStateCursor.TryGetBlockSpentTxes(0, out actualSpentTxes0));
                Assert.IsFalse(chainStateCursor.TryGetBlockSpentTxes(1, out actualSpentTxes1));
            }
        }

        public void TestContainsBlockUnmintedTxes(ITestStorageProvider provider)
        {
            var unmintedTxes0 = ImmutableList.Create(
                new UnmintedTx(txHash: (UInt256)0,
                    prevOutputTxKeys: ImmutableArray.Create(
                        new TxLookupKey(blockHash: (UInt256)0, txIndex: 0),
                        new TxLookupKey(blockHash: (UInt256)0, txIndex: 1),
                        new TxLookupKey(blockHash: (UInt256)0, txIndex: 2))),
                new UnmintedTx(txHash: (UInt256)1,
                    prevOutputTxKeys: ImmutableArray.Create(
                        new TxLookupKey(blockHash: (UInt256)0, txIndex: 3),
                        new TxLookupKey(blockHash: (UInt256)0, txIndex: 4),
                        new TxLookupKey(blockHash: (UInt256)0, txIndex: 5))));

            var unmintedTxes1 = ImmutableList.Create(
                new UnmintedTx(txHash: (UInt256)2,
                    prevOutputTxKeys: ImmutableArray.Create(
                        new TxLookupKey(blockHash: (UInt256)1, txIndex: 0),
                        new TxLookupKey(blockHash: (UInt256)1, txIndex: 1))),
                new UnmintedTx(txHash: (UInt256)3,
                    prevOutputTxKeys: ImmutableArray.Create(
                        new TxLookupKey(blockHash: (UInt256)1, txIndex: 2),
                        new TxLookupKey(blockHash: (UInt256)1, txIndex: 3))));

            using (var storageManager = provider.OpenStorageManager())
            using (var handle = storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                // begin transaction
                chainStateCursor.BeginTransaction();

                // verify presence
                Assert.IsFalse(chainStateCursor.ContainsBlockUnmintedTxes((UInt256)0));
                Assert.IsFalse(chainStateCursor.ContainsBlockUnmintedTxes((UInt256)1));

                // add unminted txes 0
                chainStateCursor.TryAddBlockUnmintedTxes((UInt256)0, unmintedTxes0);

                // verify presence
                Assert.IsTrue(chainStateCursor.ContainsBlockUnmintedTxes((UInt256)0));
                Assert.IsFalse(chainStateCursor.ContainsBlockUnmintedTxes((UInt256)1));

                // add ununminted tx 1
                chainStateCursor.TryAddBlockUnmintedTxes((UInt256)1, unmintedTxes1);

                // verify presence
                Assert.IsTrue(chainStateCursor.ContainsBlockUnmintedTxes((UInt256)0));
                Assert.IsTrue(chainStateCursor.ContainsBlockUnmintedTxes((UInt256)1));

                // remove ununminted tx 1
                chainStateCursor.TryRemoveBlockUnmintedTxes((UInt256)1);

                // verify presence
                Assert.IsTrue(chainStateCursor.ContainsBlockUnmintedTxes((UInt256)0));
                Assert.IsFalse(chainStateCursor.ContainsBlockUnmintedTxes((UInt256)1));

                // remove ununminted tx 0
                chainStateCursor.TryRemoveBlockUnmintedTxes((UInt256)0);

                // verify presence
                Assert.IsFalse(chainStateCursor.ContainsBlockUnmintedTxes((UInt256)0));
                Assert.IsFalse(chainStateCursor.ContainsBlockUnmintedTxes((UInt256)1));
            }
        }

        public void TestTryAddGetRemoveBlockUnmintedTxes(ITestStorageProvider provider)
        {
            var unmintedTxes0 = ImmutableList.Create(
                new UnmintedTx(txHash: (UInt256)0,
                    prevOutputTxKeys: ImmutableArray.Create(
                        new TxLookupKey(blockHash: (UInt256)0, txIndex: 0),
                        new TxLookupKey(blockHash: (UInt256)0, txIndex: 1),
                        new TxLookupKey(blockHash: (UInt256)0, txIndex: 2))),
                new UnmintedTx(txHash: (UInt256)1,
                    prevOutputTxKeys: ImmutableArray.Create(
                        new TxLookupKey(blockHash: (UInt256)0, txIndex: 3),
                        new TxLookupKey(blockHash: (UInt256)0, txIndex: 4),
                        new TxLookupKey(blockHash: (UInt256)0, txIndex: 5))));

            var unmintedTxes1 = ImmutableList.Create(
                new UnmintedTx(txHash: (UInt256)2,
                    prevOutputTxKeys: ImmutableArray.Create(
                        new TxLookupKey(blockHash: (UInt256)1, txIndex: 0),
                        new TxLookupKey(blockHash: (UInt256)1, txIndex: 1))),
                new UnmintedTx(txHash: (UInt256)3,
                    prevOutputTxKeys: ImmutableArray.Create(
                        new TxLookupKey(blockHash: (UInt256)1, txIndex: 2),
                        new TxLookupKey(blockHash: (UInt256)1, txIndex: 3))));

            using (var storageManager = provider.OpenStorageManager())
            using (var handle = storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                // begin transaction
                chainStateCursor.BeginTransaction();

                // verify initial empty state
                IImmutableList<UnmintedTx> actualUnmintedTxes0, actualUnmintedTxes1;
                Assert.IsFalse(chainStateCursor.TryGetBlockUnmintedTxes((UInt256)0, out actualUnmintedTxes0));
                Assert.IsFalse(chainStateCursor.TryGetBlockUnmintedTxes((UInt256)1, out actualUnmintedTxes1));

                // add unminted txes 0
                Assert.IsTrue(chainStateCursor.TryAddBlockUnmintedTxes((UInt256)0, unmintedTxes0));

                // verify unminted txes
                Assert.IsTrue(chainStateCursor.TryGetBlockUnmintedTxes((UInt256)0, out actualUnmintedTxes0));
                CollectionAssert.AreEqual(unmintedTxes0.ToList(), actualUnmintedTxes0.ToList());
                Assert.IsFalse(chainStateCursor.TryGetBlockUnmintedTxes((UInt256)1, out actualUnmintedTxes1));

                // add unminted txes 1
                Assert.IsTrue(chainStateCursor.TryAddBlockUnmintedTxes((UInt256)1, unmintedTxes1));

                // verify unminted txes
                Assert.IsTrue(chainStateCursor.TryGetBlockUnmintedTxes((UInt256)0, out actualUnmintedTxes0));
                CollectionAssert.AreEqual(unmintedTxes0.ToList(), actualUnmintedTxes0.ToList());
                Assert.IsTrue(chainStateCursor.TryGetBlockUnmintedTxes((UInt256)1, out actualUnmintedTxes1));
                CollectionAssert.AreEqual(unmintedTxes1.ToList(), actualUnmintedTxes1.ToList());

                // remove unminted txes 1
                Assert.IsTrue(chainStateCursor.TryRemoveBlockUnmintedTxes((UInt256)1));

                // verify unminted txes
                Assert.IsTrue(chainStateCursor.TryGetBlockUnmintedTxes((UInt256)0, out actualUnmintedTxes0));
                CollectionAssert.AreEqual(unmintedTxes0.ToList(), actualUnmintedTxes0.ToList());
                Assert.IsFalse(chainStateCursor.TryGetBlockUnmintedTxes((UInt256)1, out actualUnmintedTxes1));

                // remove unminted txes 0
                Assert.IsTrue(chainStateCursor.TryRemoveBlockUnmintedTxes((UInt256)0));

                // verify unminted txes
                Assert.IsFalse(chainStateCursor.TryGetBlockUnmintedTxes((UInt256)0, out actualUnmintedTxes0));
                Assert.IsFalse(chainStateCursor.TryGetBlockUnmintedTxes((UInt256)1, out actualUnmintedTxes1));
            }
        }

        public void TestFlush(ITestStorageProvider provider)
        {
            Assert.Inconclusive("TODO");
        }

        public void TestDefragment(ITestStorageProvider provider)
        {
            Assert.Inconclusive("TODO");
        }

        /// <summary>
        /// Verify that chain state cursor does not allow use outside of a transaction.
        /// </summary>
        /// <param name="provider"></param>
        public void TestOperationOutsideTransaction(ITestStorageProvider provider)
        {
            var unspentTx = new UnspentTx(txHash: UInt256.Zero, blockIndex: 0, txIndex: 0, outputStates: new OutputStates(1, OutputState.Unspent), txOutputs: ImmutableArray<TxOutput>.Empty);

            using (var storageManager = provider.OpenStorageManager())
            using (var handle = storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                Assert.IsFalse(chainStateCursor.InTransaction);

                var actions = new Action[]
                {
                    () => { var x = chainStateCursor.ChainTip; },
                    () => { chainStateCursor.ChainTip = RandomData.RandomChainedHeader(); },
                    () => { var x = chainStateCursor.UnspentTxCount; },
                    () => { chainStateCursor.UnspentTxCount = 0; },
                    () => { var x = chainStateCursor.UnspentOutputCount; },
                    () => { chainStateCursor.UnspentOutputCount = 0; },
                    () => { var x = chainStateCursor.TotalTxCount; },
                    () => { chainStateCursor.TotalTxCount = 0; },
                    () => { var x = chainStateCursor.TotalInputCount; },
                    () => { chainStateCursor.TotalInputCount = 0; },
                    () => { var x = chainStateCursor. TotalOutputCount; },
                    () => { chainStateCursor. TotalOutputCount = 0; },
                    () => { var x = chainStateCursor.ContainsUnspentTx(UInt256.Zero); },
                    () => { UnspentTx _; chainStateCursor.TryGetUnspentTx(UInt256.Zero, out _); },
                    () => { chainStateCursor.TryAddUnspentTx(unspentTx); },
                    () => { chainStateCursor.TryRemoveUnspentTx(UInt256.Zero); },
                    () => { chainStateCursor.TryUpdateUnspentTx(unspentTx); },
                    () => { chainStateCursor.ReadUnspentTransactions(); },
                    () => { chainStateCursor.ContainsBlockSpentTxes(0); },
                    () => { BlockSpentTxes _; chainStateCursor.TryGetBlockSpentTxes(0, out _); },
                    () => { chainStateCursor.TryAddBlockSpentTxes(0, BlockSpentTxes.CreateRange(Enumerable.Empty<SpentTx>())); },
                    () => { chainStateCursor.TryRemoveBlockSpentTxes(0); },
                    () => { chainStateCursor.ContainsBlockUnmintedTxes(UInt256.Zero); },
                    () => { IImmutableList<UnmintedTx> _; chainStateCursor.TryGetBlockUnmintedTxes(UInt256.Zero, out _); },
                    () => { chainStateCursor.TryAddBlockUnmintedTxes(UInt256.Zero  , ImmutableList<UnmintedTx>.Empty); },
                    () => { chainStateCursor.TryRemoveBlockUnmintedTxes(UInt256.Zero); },
                };

                foreach (var action in actions)
                    AssertMethods.AssertThrows<InvalidOperationException>(action);
            }
        }

        /// <summary>
        /// Verify that chain state cursor does not allow write operations in read-only transaction.
        /// </summary>
        /// <param name="provider"></param>
        public void TestWriteOperationInReadonlyTransaction(ITestStorageProvider provider)
        {
            var unspentTx = new UnspentTx(txHash: UInt256.Zero, blockIndex: 0, txIndex: 0, outputStates: new OutputStates(1, OutputState.Unspent), txOutputs: ImmutableArray<TxOutput>.Empty);

            using (var storageManager = provider.OpenStorageManager())
            using (var handle = storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                chainStateCursor.BeginTransaction(readOnly: true);

                var actions = new Action[]
                {
                    () => { chainStateCursor.ChainTip = RandomData.RandomChainedHeader(); },
                    () => { chainStateCursor.UnspentTxCount = 0; },
                    () => { chainStateCursor.UnspentOutputCount = 0; },
                    () => { chainStateCursor.TotalTxCount = 0; },
                    () => { chainStateCursor.TotalInputCount = 0; },
                    () => { chainStateCursor. TotalOutputCount = 0; },
                    () => { chainStateCursor.TryAddUnspentTx(unspentTx); },
                    () => { chainStateCursor.TryRemoveUnspentTx(UInt256.Zero); },
                    () => { chainStateCursor.TryUpdateUnspentTx(unspentTx); },
                    () => { chainStateCursor.TryAddBlockSpentTxes(0, BlockSpentTxes.CreateRange(Enumerable.Empty<SpentTx>())); },
                    () => { chainStateCursor.TryRemoveBlockSpentTxes(0); },
                    () => { chainStateCursor.TryAddBlockUnmintedTxes(UInt256.Zero  , ImmutableList<UnmintedTx>.Empty); },
                    () => { chainStateCursor.TryRemoveBlockUnmintedTxes(UInt256.Zero); },
                };

                foreach (var action in actions)
                    AssertMethods.AssertThrows<InvalidOperationException>(action);

                chainStateCursor.RollbackTransaction();
            }
        }

        public void TestAccessAcrossThreads(ITestStorageProvider provider)
        {
            var unspentTx = new UnspentTx(txHash: UInt256.Zero, blockIndex: 0, txIndex: 0, outputStates: new OutputStates(1, OutputState.Unspent), txOutputs: ImmutableArray<TxOutput>.Empty);

            using (var storageManager = provider.OpenStorageManager())
            using (var handle = storageManager.OpenChainStateCursor())
            using (var thread1Done = new AutoResetEvent(false))
            using (var thread2Done = new AutoResetEvent(false))
            {
                var chainStateCursor = handle.Item;

                var thread1Actions = new BlockingCollection<Action>();
                var thread2Actions = new BlockingCollection<Action>();

                var thread1 = new Thread(() =>
                    {
                        foreach (var action in thread1Actions.GetConsumingEnumerable())
                        {
                            action();
                            thread1Done.Set();
                        }
                    });

                var thread2 = new Thread(() =>
                {
                    foreach (var action in thread2Actions.GetConsumingEnumerable())
                    {
                        action();
                        thread2Done.Set();
                    }
                });

                thread1.Start();
                thread2.Start();

                // begin transaction on thread #1
                thread1Actions.Add(() =>
                    chainStateCursor.BeginTransaction());
                thread1Done.WaitOne();

                // commit transaction on thread #2
                thread2Actions.Add(() =>
                    chainStateCursor.CommitTransaction());
                thread2Done.WaitOne();

                // begin transaction on thread #1
                thread1Actions.Add(() =>
                    chainStateCursor.BeginTransaction());
                thread1Done.WaitOne();

                // rollback transaction on thread #2
                thread2Actions.Add(() =>
                    chainStateCursor.RollbackTransaction());
                thread2Done.WaitOne();

                thread1Actions.CompleteAdding();
                thread2Actions.CompleteAdding();

                thread1.Join();
                thread2.Join();
            }
        }
    }
}
