using BitSharp.Core.Domain;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Linq;

namespace BitSharp.Core.Test.Storage
{
    [TestClass]
    public class IBlockStorageTest : StorageProviderTest
    {
        [TestMethod]
        public void TestContainsChainedHeader()
        {
            RunTest(TestContainsChainedHeader);
        }

        [TestMethod]
        public void TestTryAddRemoveChainedHeader()
        {
            RunTest(TestTryAddRemoveChainedHeader);
        }

        [TestMethod]
        public void TestTryGetChainedHeader()
        {
            RunTest(TestTryGetChainedHeader);
        }

        [TestMethod]
        public void TestFindMaxTotalWork()
        {
            RunTest(TestFindMaxTotalWork);
        }

        [TestMethod]
        public void TestReadChainedHeaders()
        {
            RunTest(TestReadChainedHeaders);
        }

        [TestMethod]
        public void TestMarkBlockInvalid()
        {
            RunTest(TestMarkBlockInvalid);
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

        // IBlockStorage.ContainsChainedHeader
        private void TestContainsChainedHeader(ITestStorageProvider provider)
        {
            using (var storageManager = provider.OpenStorageManager())
            {
                var blockStorage = storageManager.BlockStorage;

                // create a chained header
                var fakeHeaders = new FakeHeaders();
                var chainedHeader = fakeHeaders.GenesisChained();

                // header should not be present
                Assert.IsFalse(blockStorage.ContainsChainedHeader(chainedHeader.Hash));

                // add the header
                blockStorage.TryAddChainedHeader(chainedHeader);

                // header should be present
                Assert.IsTrue(blockStorage.ContainsChainedHeader(chainedHeader.Hash));

                // remove the header
                blockStorage.TryRemoveChainedHeader(chainedHeader.Hash);

                // header should not be present
                Assert.IsFalse(blockStorage.ContainsChainedHeader(chainedHeader.Hash));
            }
        }

        // IBlockStorage.TryAddChainedHeader
        // IBlockStorage.TryRemoveChainedHeader
        private void TestTryAddRemoveChainedHeader(ITestStorageProvider provider)
        {
            using (var storageManager = provider.OpenStorageManager())
            {
                var blockStorage = storageManager.BlockStorage;

                // create a chained header
                var fakeHeaders = new FakeHeaders();
                var chainedHeader = fakeHeaders.GenesisChained();

                // verify header can be added
                Assert.IsTrue(blockStorage.TryAddChainedHeader(chainedHeader));
                Assert.IsTrue(blockStorage.ContainsChainedHeader(chainedHeader.Hash));

                // verify header cannot be added again
                Assert.IsFalse(blockStorage.TryAddChainedHeader(chainedHeader));

                // remove the header
                Assert.IsTrue(blockStorage.TryRemoveChainedHeader(chainedHeader.Hash));
                Assert.IsFalse(blockStorage.ContainsChainedHeader(chainedHeader.Hash));

                // verify header cannot be removed again
                Assert.IsFalse(blockStorage.TryRemoveChainedHeader(chainedHeader.Hash));

                // verify header can be added again, after being removed
                Assert.IsTrue(blockStorage.TryAddChainedHeader(chainedHeader));
                Assert.IsTrue(blockStorage.ContainsChainedHeader(chainedHeader.Hash));

                // verify header can be removed again, after being added again
                Assert.IsTrue(blockStorage.TryRemoveChainedHeader(chainedHeader.Hash));
                Assert.IsFalse(blockStorage.ContainsChainedHeader(chainedHeader.Hash));
            }
        }

        // IBlockStorage.TryGetChainedHeader
        private void TestTryGetChainedHeader(ITestStorageProvider provider)
        {
            using (var storageManager = provider.OpenStorageManager())
            {
                var blockStorage = storageManager.BlockStorage;

                // create a chained header
                var fakeHeaders = new FakeHeaders();
                var expectedChainedHeader = fakeHeaders.GenesisChained();

                // add header
                blockStorage.TryAddChainedHeader(expectedChainedHeader);

                // retrieve header
                ChainedHeader actualChainedHeader;
                Assert.IsTrue(blockStorage.TryGetChainedHeader(expectedChainedHeader.Hash, out actualChainedHeader));

                // verify retrieved header matches stored header
                Assert.AreEqual(expectedChainedHeader, actualChainedHeader);
            }
        }

        // IBlockStorage.FindMaxTotalWork
        private void TestFindMaxTotalWork(ITestStorageProvider provider)
        {
            using (var storageManager = provider.OpenStorageManager())
            {
                var blockStorage = storageManager.BlockStorage;

                // create chained headers
                var fakeHeaders = new FakeHeaders();
                var chainedHeader0 = fakeHeaders.GenesisChained();
                var chainedHeader1 = fakeHeaders.NextChained();
                var chainedHeader2 = fakeHeaders.NextChained();

                // verify initial null state
                Assert.IsNull(blockStorage.FindMaxTotalWork());

                // add headers and verify max total work

                // 0
                blockStorage.TryAddChainedHeader(chainedHeader0);
                Assert.AreEqual(chainedHeader0, blockStorage.FindMaxTotalWork());

                // 1
                blockStorage.TryAddChainedHeader(chainedHeader1);
                Assert.AreEqual(chainedHeader1, blockStorage.FindMaxTotalWork());

                // 2
                blockStorage.TryAddChainedHeader(chainedHeader2);
                Assert.AreEqual(chainedHeader2, blockStorage.FindMaxTotalWork());

                // remove headers and verify max total work

                // 2
                blockStorage.TryRemoveChainedHeader(chainedHeader2.Hash);
                Assert.AreEqual(chainedHeader1, blockStorage.FindMaxTotalWork());

                // 1
                blockStorage.TryRemoveChainedHeader(chainedHeader1.Hash);
                Assert.AreEqual(chainedHeader0, blockStorage.FindMaxTotalWork());

                // 0
                blockStorage.TryRemoveChainedHeader(chainedHeader0.Hash);
                Assert.IsNull(blockStorage.FindMaxTotalWork());
            }
        }

        // IBlockStorage.ReadChainedHeaders
        private void TestReadChainedHeaders(ITestStorageProvider provider)
        {
            using (var storageManager = provider.OpenStorageManager())
            {
                var blockStorage = storageManager.BlockStorage;

                // create chained headers
                var fakeHeaders = new FakeHeaders();
                var chainedHeader0 = fakeHeaders.GenesisChained();
                var chainedHeader1 = fakeHeaders.NextChained();
                var chainedHeader2 = fakeHeaders.NextChained();

                // verify initial empty state
                Assert.AreEqual(0, blockStorage.ReadChainedHeaders().ToList().Count);

                // add headers and verify reading them

                // 0
                blockStorage.TryAddChainedHeader(chainedHeader0);
                CollectionAssert.AreEquivalent(new[] { chainedHeader0 }, blockStorage.ReadChainedHeaders().ToList());

                // 1
                blockStorage.TryAddChainedHeader(chainedHeader1);
                CollectionAssert.AreEquivalent(new[] { chainedHeader0, chainedHeader1 }, blockStorage.ReadChainedHeaders().ToList());

                // 2
                blockStorage.TryAddChainedHeader(chainedHeader2);
                CollectionAssert.AreEquivalent(new[] { chainedHeader0, chainedHeader1, chainedHeader2 }, blockStorage.ReadChainedHeaders().ToList());

                // remove headers and verify reading them

                // 2
                blockStorage.TryRemoveChainedHeader(chainedHeader2.Hash);
                CollectionAssert.AreEquivalent(new[] { chainedHeader0, chainedHeader1 }, blockStorage.ReadChainedHeaders().ToList());

                // 1
                blockStorage.TryRemoveChainedHeader(chainedHeader1.Hash);
                CollectionAssert.AreEquivalent(new[] { chainedHeader0 }, blockStorage.ReadChainedHeaders().ToList());

                // 0
                blockStorage.TryRemoveChainedHeader(chainedHeader0.Hash);
                Assert.AreEqual(0, blockStorage.ReadChainedHeaders().ToList().Count);
            }
        }

        // IBlockStorage.IsBlockInvalid
        // IBlockStorage.MarkBlockInvalid
        private void TestMarkBlockInvalid(ITestStorageProvider provider)
        {
            using (var storageManager = provider.OpenStorageManager())
            {
                var blockStorage = storageManager.BlockStorage;

                // create chained headers
                var fakeHeaders = new FakeHeaders();
                var chainedHeader0 = fakeHeaders.GenesisChained();
                var chainedHeader1 = fakeHeaders.NextChained();

                // add headers
                blockStorage.TryAddChainedHeader(chainedHeader0);
                blockStorage.TryAddChainedHeader(chainedHeader1);

                // verify no blocks invalid
                Assert.IsFalse(blockStorage.IsBlockInvalid(chainedHeader0.Hash));
                Assert.IsFalse(blockStorage.IsBlockInvalid(chainedHeader1.Hash));

                // mark blocks invalid and verify

                // 0
                blockStorage.MarkBlockInvalid(chainedHeader0.Hash);
                Assert.IsTrue(blockStorage.IsBlockInvalid(chainedHeader0.Hash));
                Assert.IsFalse(blockStorage.IsBlockInvalid(chainedHeader1.Hash));

                // 1
                blockStorage.MarkBlockInvalid(chainedHeader1.Hash);
                Assert.IsTrue(blockStorage.IsBlockInvalid(chainedHeader0.Hash));
                Assert.IsTrue(blockStorage.IsBlockInvalid(chainedHeader1.Hash));
            }
        }

        // IBlockStorage.Flush
        private void TestFlush(ITestStorageProvider provider)
        {
            using (var storageManager = provider.OpenStorageManager())
            {
                var blockStorage = storageManager.BlockStorage;

                Assert.Inconclusive("TODO");
            }
        }

        // IBlockStorage.Defragment
        private void TestDefragment(ITestStorageProvider provider)
        {
            using (var storageManager = provider.OpenStorageManager())
            {
                var blockStorage = storageManager.BlockStorage;

                Assert.Inconclusive("TODO");
            }
        }

    }
}
