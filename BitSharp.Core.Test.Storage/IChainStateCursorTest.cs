﻿using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using BitSharp.Core.Test;
using BitSharp.Core.Test.Rules;
using BitSharp.Esent.Test;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NLog;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

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
        public void TestReadChain()
        {
            RunTest(TestReadChain);
        }

        [TestMethod]
        public void TestGetChainTip()
        {
            RunTest(TestGetChainTip);
        }

        [TestMethod]
        public void TestAddChainedHeader()
        {
            RunTest(TestAddChainedHeader);
        }

        [TestMethod]
        public void TestRemoveChainedHeader()
        {
            RunTest(TestRemoveChainedHeader);
        }

        [TestMethod]
        public void TestUnspentTxCount()
        {
            RunTest(TestUnspentTxCount);
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
        public void TestRemoveSpentTransactionsToHeight()
        {
            RunTest(TestRemoveSpentTransactionsToHeight);
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

        private void TestTransactionIsolation(ITestStorageProvider provider)
        {
            var fakeHeaders = new FakeHeaders();
            var chainedHeader0 = fakeHeaders.GenesisChained();

            // open two chain state cursors
            using (var storageManager = provider.OpenStorageManager())
            using (var handle1 = storageManager.OpenChainStateCursor())
            using (var handle2 = storageManager.OpenChainStateCursor())
            {
                var chainStateCursor1 = handle1.Item;
                var chainStateCursor2 = handle2.Item;

                // open transactions on both cursors
                chainStateCursor1.BeginTransaction();
                chainStateCursor2.BeginTransaction();

                // verify initial empty chain
                Assert.AreEqual(0, chainStateCursor1.ReadChain().Count());
                Assert.AreEqual(0, chainStateCursor2.ReadChain().Count());

                // add a header on cursor 1
                chainStateCursor1.AddChainedHeader(chainedHeader0);

                // verify cursor 1 sees the new header while cursor 2 does not
                CollectionAssert.AreEqual(new[] { chainedHeader0 }, chainStateCursor1.ReadChain().ToList());
                Assert.AreEqual(0, chainStateCursor2.ReadChain().Count());

                // commit cursor 1
                chainStateCursor1.CommitTransaction();

                // verify cursor 1 sees the new header while cursor 2 does not
                CollectionAssert.AreEqual(new[] { chainedHeader0 }, chainStateCursor1.ReadChain().ToList());
                Assert.AreEqual(0, chainStateCursor2.ReadChain().Count());

                // commit cursor 2
                chainStateCursor2.CommitTransaction();

                // verify cursor 2 now sees the new header
                CollectionAssert.AreEqual(new[] { chainedHeader0 }, chainStateCursor2.ReadChain().ToList());
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
            var unspentTx = new UnspentTx(txHash: 0, blockIndex: 0, txIndex: 0, outputStates: new OutputStates(1, OutputState.Unspent));
            var spentTxes = ImmutableList.Create(new SpentTx(txHash: 1, confirmedBlockIndex: 0, txIndex: 0, outputCount: 1, spentBlockIndex: 0));

            using (var storageManager = provider.OpenStorageManager())
            using (var handle = storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                // begin transaction
                chainStateCursor.BeginTransaction();

                // add data
                chainStateCursor.AddChainedHeader(chainedHeader0);
                chainStateCursor.TryAddUnspentTx(unspentTx);
                chainStateCursor.TryAddBlockSpentTxes(0, spentTxes);

                // verify data
                Assert.AreEqual(chainedHeader0, chainStateCursor.GetChainTip());
                Assert.AreEqual(1, chainStateCursor.UnspentTxCount);

                UnspentTx actualUnspentTx;
                Assert.IsTrue(chainStateCursor.TryGetUnspentTx(unspentTx.TxHash, out actualUnspentTx));
                Assert.AreEqual(unspentTx, actualUnspentTx);

                IImmutableList<SpentTx> actualSpentTxes;
                Assert.IsTrue(chainStateCursor.TryGetBlockSpentTxes(0, out actualSpentTxes));
                CollectionAssert.AreEqual((ICollection)spentTxes, (ICollection)actualSpentTxes);

                // commit transaction
                chainStateCursor.CommitTransaction();

                // verify data
                Assert.AreEqual(chainedHeader0, chainStateCursor.GetChainTip());
                Assert.AreEqual(1, chainStateCursor.UnspentTxCount);

                Assert.IsTrue(chainStateCursor.TryGetUnspentTx(unspentTx.TxHash, out actualUnspentTx));
                Assert.AreEqual(unspentTx, actualUnspentTx);

                Assert.IsTrue(chainStateCursor.TryGetBlockSpentTxes(0, out actualSpentTxes));
                CollectionAssert.AreEqual((ICollection)spentTxes, (ICollection)actualSpentTxes);
            }
        }

        private void TestRollbackTransaction(ITestStorageProvider provider)
        {
            var fakeHeaders = new FakeHeaders();
            var chainedHeader0 = fakeHeaders.GenesisChained();
            var chainedHeader1 = fakeHeaders.NextChained();
            var chainedHeader2 = fakeHeaders.NextChained();

            var unspentTx = new UnspentTx(txHash: 0, blockIndex: 0, txIndex: 0, outputStates: new OutputStates(1, OutputState.Unspent));

            var spentTxes = ImmutableList.Create(new SpentTx(txHash: 1, confirmedBlockIndex: 0, txIndex: 0, outputCount: 1, spentBlockIndex: 0));

            using (var storageManager = provider.OpenStorageManager())
            using (var handle = storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                // begin transaction
                chainStateCursor.BeginTransaction();

                // add header 0
                chainStateCursor.AddChainedHeader(chainedHeader0);

                // verify chain
                CollectionAssert.AreEqual(new[] { chainedHeader0 }, chainStateCursor.ReadChain().ToList());

                // add unspent tx
                chainStateCursor.TryAddUnspentTx(unspentTx);

                // verify unspent tx
                Assert.IsTrue(chainStateCursor.ContainsUnspentTx(unspentTx.TxHash));
                Assert.AreEqual(1, chainStateCursor.UnspentTxCount);

                // add spent txes
                chainStateCursor.TryAddBlockSpentTxes(0, spentTxes);

                // verify spent txes
                IImmutableList<SpentTx> actualSpentTxes;
                Assert.IsTrue(chainStateCursor.TryGetBlockSpentTxes(0, out actualSpentTxes));
                CollectionAssert.AreEqual(spentTxes, (ICollection)actualSpentTxes);

                // rollback transaction
                chainStateCursor.RollbackTransaction();

                // verify chain
                Assert.AreEqual(0, chainStateCursor.ReadChain().Count());

                // verify unspent tx
                Assert.IsFalse(chainStateCursor.ContainsUnspentTx(unspentTx.TxHash));
                Assert.AreEqual(0, chainStateCursor.UnspentTxCount);

                // verify spent txes
                Assert.IsFalse(chainStateCursor.TryGetBlockSpentTxes(0, out actualSpentTxes));
            }
        }

        private void TestReadChain(ITestStorageProvider provider)
        {
            var fakeHeaders = new FakeHeaders();
            var chainedHeader0 = fakeHeaders.GenesisChained();
            var chainedHeader1 = fakeHeaders.NextChained();
            var chainedHeader2 = fakeHeaders.NextChained();

            using (var storageManager = provider.OpenStorageManager())
            using (var handle = storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                // begin transaction
                chainStateCursor.BeginTransaction();

                // verify initial empty chain
                Assert.AreEqual(0, chainStateCursor.ReadChain().Count());

                // add header 0
                chainStateCursor.AddChainedHeader(chainedHeader0);

                // verify chain
                CollectionAssert.AreEqual(new[] { chainedHeader0 }, chainStateCursor.ReadChain().ToList());

                // add header 1
                chainStateCursor.AddChainedHeader(chainedHeader1);

                // verify chain
                CollectionAssert.AreEqual(new[] { chainedHeader0, chainedHeader1 }, chainStateCursor.ReadChain().ToList());

                // add header 2
                chainStateCursor.AddChainedHeader(chainedHeader2);

                // verify chain
                CollectionAssert.AreEqual(new[] { chainedHeader0, chainedHeader1, chainedHeader2 }, chainStateCursor.ReadChain().ToList());

                // remove header 2
                chainStateCursor.RemoveChainedHeader(chainedHeader2);

                // verify chain
                CollectionAssert.AreEqual(new[] { chainedHeader0, chainedHeader1 }, chainStateCursor.ReadChain().ToList());

                // remove header 1
                chainStateCursor.RemoveChainedHeader(chainedHeader1);

                // verify chain
                CollectionAssert.AreEqual(new[] { chainedHeader0 }, chainStateCursor.ReadChain().ToList());

                // remove header 0
                chainStateCursor.RemoveChainedHeader(chainedHeader0);

                // verify chain
                Assert.AreEqual(0, chainStateCursor.ReadChain().Count());
            }
        }

        private void TestGetChainTip(ITestStorageProvider provider)
        {
            var fakeHeaders = new FakeHeaders();
            var chainedHeader0 = fakeHeaders.GenesisChained();
            var chainedHeader1 = fakeHeaders.NextChained();
            var chainedHeader2 = fakeHeaders.NextChained();

            using (var storageManager = provider.OpenStorageManager())
            using (var handle = storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                // begin transaction
                chainStateCursor.BeginTransaction();

                // verify initial empty chain
                Assert.IsNull(chainStateCursor.GetChainTip());

                // add header 0
                chainStateCursor.AddChainedHeader(chainedHeader0);

                // verify chain tip
                Assert.AreEqual(chainedHeader0, chainStateCursor.GetChainTip());

                // add header 1
                chainStateCursor.AddChainedHeader(chainedHeader1);

                // verify chain tip
                Assert.AreEqual(chainedHeader1, chainStateCursor.GetChainTip());

                // add header 2
                chainStateCursor.AddChainedHeader(chainedHeader2);

                // verify chain tip
                Assert.AreEqual(chainedHeader2, chainStateCursor.GetChainTip());

                // remove header 2
                chainStateCursor.RemoveChainedHeader(chainedHeader2);

                // verify chain tip
                Assert.AreEqual(chainedHeader1, chainStateCursor.GetChainTip());

                // remove header 1
                chainStateCursor.RemoveChainedHeader(chainedHeader1);

                // verify chain tip
                Assert.AreEqual(chainedHeader0, chainStateCursor.GetChainTip());

                // remove header 0
                chainStateCursor.RemoveChainedHeader(chainedHeader0);

                // verify chain tip
                Assert.IsNull(chainStateCursor.GetChainTip());
            }
        }

        private void TestAddChainedHeader(ITestStorageProvider provider)
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

                // verify initial empty chain
                Assert.AreEqual(0, chainStateCursor.ReadChain().Count());

                // add header 0
                chainStateCursor.AddChainedHeader(chainedHeader0);

                // verify chain
                CollectionAssert.AreEqual(new[] { chainedHeader0 }, chainStateCursor.ReadChain().ToList());

                // try to add header 0 again
                AssertThrows<InvalidOperationException>(() => chainStateCursor.AddChainedHeader(chainedHeader0));

                // verify chain
                CollectionAssert.AreEqual(new[] { chainedHeader0 }, chainStateCursor.ReadChain().ToList());

                // add header 1
                chainStateCursor.AddChainedHeader(chainedHeader1);

                // verify chain
                CollectionAssert.AreEqual(new[] { chainedHeader0, chainedHeader1 }, chainStateCursor.ReadChain().ToList());
            }
        }

        private void TestRemoveChainedHeader(ITestStorageProvider provider)
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

                // add headers
                chainStateCursor.AddChainedHeader(chainedHeader0);
                chainStateCursor.AddChainedHeader(chainedHeader1);

                // verify chain
                CollectionAssert.AreEqual(new[] { chainedHeader0, chainedHeader1 }, chainStateCursor.ReadChain().ToList());

                // remove header 1
                chainStateCursor.RemoveChainedHeader(chainedHeader1);

                // verify chain
                CollectionAssert.AreEqual(new[] { chainedHeader0 }, chainStateCursor.ReadChain().ToList());

                // try to remove header 1 again
                AssertThrows<InvalidOperationException>(() => chainStateCursor.RemoveChainedHeader(chainedHeader1));

                // verify chain
                CollectionAssert.AreEqual(new[] { chainedHeader0 }, chainStateCursor.ReadChain().ToList());

                // remove header 0
                chainStateCursor.RemoveChainedHeader(chainedHeader0);

                // verify chain
                Assert.AreEqual(0, chainStateCursor.ReadChain().Count());
            }
        }

        private void TestUnspentTxCount(ITestStorageProvider provider)
        {
            var unspentTx0 = new UnspentTx(txHash: 0, blockIndex: 0, txIndex: 0, outputStates: new OutputStates(1, OutputState.Unspent));
            var unspentTx1 = new UnspentTx(txHash: 1, blockIndex: 0, txIndex: 0, outputStates: new OutputStates(1, OutputState.Unspent));
            var unspentTx2 = new UnspentTx(txHash: 2, blockIndex: 0, txIndex: 0, outputStates: new OutputStates(1, OutputState.Unspent));

            using (var storageManager = provider.OpenStorageManager())
            using (var handle = storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                // begin transaction
                chainStateCursor.BeginTransaction();

                // verify initial count
                Assert.AreEqual(0, chainStateCursor.UnspentTxCount);

                // add unspent tx 0
                chainStateCursor.TryAddUnspentTx(unspentTx0);

                // verify count
                Assert.AreEqual(1, chainStateCursor.UnspentTxCount);

                // add unspent tx 1
                chainStateCursor.TryAddUnspentTx(unspentTx1);

                // verify count
                Assert.AreEqual(2, chainStateCursor.UnspentTxCount);

                // add unspent tx 2
                chainStateCursor.TryAddUnspentTx(unspentTx2);

                // verify count
                Assert.AreEqual(3, chainStateCursor.UnspentTxCount);

                // remove unspent tx 2
                chainStateCursor.TryRemoveUnspentTx(unspentTx2.TxHash);

                // verify count
                Assert.AreEqual(2, chainStateCursor.UnspentTxCount);

                // remove unspent tx 1
                chainStateCursor.TryRemoveUnspentTx(unspentTx1.TxHash);

                // verify count
                Assert.AreEqual(1, chainStateCursor.UnspentTxCount);

                // remove unspent tx 0
                chainStateCursor.TryRemoveUnspentTx(unspentTx0.TxHash);

                // verify count
                Assert.AreEqual(0, chainStateCursor.UnspentTxCount);
            }
        }

        private void TestContainsUnspentTx(ITestStorageProvider provider)
        {
            var unspentTx0 = new UnspentTx(txHash: 0, blockIndex: 0, txIndex: 0, outputStates: new OutputStates(1, OutputState.Unspent));
            var unspentTx1 = new UnspentTx(txHash: 1, blockIndex: 0, txIndex: 0, outputStates: new OutputStates(1, OutputState.Unspent));

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
            var unspentTx0 = new UnspentTx(txHash: 0, blockIndex: 0, txIndex: 0, outputStates: new OutputStates(1, OutputState.Unspent));
            var unspentTx1 = new UnspentTx(txHash: 1, blockIndex: 0, txIndex: 0, outputStates: new OutputStates(1, OutputState.Unspent));

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
            var unspentTx = new UnspentTx(txHash: 0, blockIndex: 0, txIndex: 0, outputStates: new OutputStates(1, OutputState.Unspent));
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
            var unspentTx0 = new UnspentTx(txHash: 0, blockIndex: 0, txIndex: 0, outputStates: new OutputStates(1, OutputState.Unspent));
            var unspentTx1 = new UnspentTx(txHash: 1, blockIndex: 0, txIndex: 0, outputStates: new OutputStates(1, OutputState.Unspent));
            var unspentTx2 = new UnspentTx(txHash: 2, blockIndex: 0, txIndex: 0, outputStates: new OutputStates(1, OutputState.Unspent));

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
            var spentTxes0 = ImmutableList.Create(
                new SpentTx(txHash: 0, confirmedBlockIndex: 0, txIndex: 0, outputCount: 1, spentBlockIndex: 0),
                new SpentTx(txHash: 1, confirmedBlockIndex: 0, txIndex: 1, outputCount: 2, spentBlockIndex: 0),
                new SpentTx(txHash: 2, confirmedBlockIndex: 0, txIndex: 2, outputCount: 3, spentBlockIndex: 0));

            var spentTxes1 = ImmutableList.Create(
                new SpentTx(txHash: 100, confirmedBlockIndex: 1, txIndex: 0, outputCount: 1, spentBlockIndex: 1),
                new SpentTx(txHash: 101, confirmedBlockIndex: 1, txIndex: 1, outputCount: 2, spentBlockIndex: 1));

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
            var spentTxes0 = ImmutableList.Create(
                new SpentTx(txHash: 0, confirmedBlockIndex: 0, txIndex: 0, outputCount: 1, spentBlockIndex: 0),
                new SpentTx(txHash: 1, confirmedBlockIndex: 0, txIndex: 1, outputCount: 2, spentBlockIndex: 0),
                new SpentTx(txHash: 2, confirmedBlockIndex: 0, txIndex: 2, outputCount: 3, spentBlockIndex: 0));

            var spentTxes1 = ImmutableList.Create(
                new SpentTx(txHash: 100, confirmedBlockIndex: 1, txIndex: 0, outputCount: 1, spentBlockIndex: 1),
                new SpentTx(txHash: 101, confirmedBlockIndex: 1, txIndex: 1, outputCount: 2, spentBlockIndex: 1));

            using (var storageManager = provider.OpenStorageManager())
            using (var handle = storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;

                // begin transaction
                chainStateCursor.BeginTransaction();

                // verify initial empty state
                IImmutableList<SpentTx> actualSpentTxes0, actualSpentTxes1;
                Assert.IsFalse(chainStateCursor.TryGetBlockSpentTxes(0, out actualSpentTxes0));
                Assert.IsFalse(chainStateCursor.TryGetBlockSpentTxes(1, out actualSpentTxes1));

                // add spent txes 0
                Assert.IsTrue(chainStateCursor.TryAddBlockSpentTxes(0, spentTxes0));

                // verify spent txes
                Assert.IsTrue(chainStateCursor.TryGetBlockSpentTxes(0, out actualSpentTxes0));
                CollectionAssert.AreEqual(spentTxes0, (ICollection)actualSpentTxes0);
                Assert.IsFalse(chainStateCursor.TryGetBlockSpentTxes(1, out actualSpentTxes1));

                // add spent txes 1
                Assert.IsTrue(chainStateCursor.TryAddBlockSpentTxes(1, spentTxes1));

                // verify spent txes
                Assert.IsTrue(chainStateCursor.TryGetBlockSpentTxes(0, out actualSpentTxes0));
                CollectionAssert.AreEqual(spentTxes0, (ICollection)actualSpentTxes0);
                Assert.IsTrue(chainStateCursor.TryGetBlockSpentTxes(1, out actualSpentTxes1));
                CollectionAssert.AreEqual(spentTxes1, (ICollection)actualSpentTxes1);

                // remove spent txes 1
                Assert.IsTrue(chainStateCursor.TryRemoveBlockSpentTxes(1));

                // verify spent txes
                Assert.IsTrue(chainStateCursor.TryGetBlockSpentTxes(0, out actualSpentTxes0));
                CollectionAssert.AreEqual(spentTxes0, (ICollection)actualSpentTxes0);
                Assert.IsFalse(chainStateCursor.TryGetBlockSpentTxes(1, out actualSpentTxes1));

                // remove spent txes 0
                Assert.IsTrue(chainStateCursor.TryRemoveBlockSpentTxes(0));

                // verify spent txes
                Assert.IsFalse(chainStateCursor.TryGetBlockSpentTxes(0, out actualSpentTxes0));
                Assert.IsFalse(chainStateCursor.TryGetBlockSpentTxes(1, out actualSpentTxes1));
            }
        }

        public void TestRemoveSpentTransactionsToHeight(ITestStorageProvider provider)
        {
            Assert.Inconclusive("TODO");
        }

        public void TestFlush(ITestStorageProvider provider)
        {
            Assert.Inconclusive("TODO");
        }

        public void TestDefragment(ITestStorageProvider provider)
        {
            Assert.Inconclusive("TODO");
        }

        private void AssertThrows<T>(Action action) where T : Exception
        {
            try
            {
                action();
                Assert.Fail("No exception thrown, expected: {0}".Format2(typeof(T).Name));
            }
            catch (UnitTestAssertException) { throw; }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(T), "Unexpected exeption thrown.");
            }
        }
    }
}
