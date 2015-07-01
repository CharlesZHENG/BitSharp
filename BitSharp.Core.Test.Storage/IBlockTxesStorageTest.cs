using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage;
using BitSharp.Core.Test.Rules;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Ninject;
using NLog;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;

namespace BitSharp.Core.Test.Storage
{
    [TestClass]
    public class IBlockTxesStorageTest : StorageProviderTest
    {
        [TestMethod]
        public void TestBlockCount()
        {
            RunTest(TestBlockCount);
        }

        [TestMethod]
        public void TestContainsBlock()
        {
            RunTest(TestContainsBlock);
        }

        [TestMethod]
        public void TestTryAddRemoveBlockTransactions()
        {
            RunTest(TestTryAddRemoveBlockTransactions);
        }

        [TestMethod]
        public void TestTryGetTransaction()
        {
            RunTest(TestTryGetTransaction);
        }

        [TestMethod]
        public void TestReadBlockTransactions()
        {
            RunTest(TestReadBlockTransactions);
        }

        [TestMethod]
        public void TestPruneElements()
        {
            RunTest(TestPruneElements);
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

        // IBlockTxesStorage.BlockCount
        private void TestBlockCount(ITestStorageProvider provider)
        {
            using (var storageManager = provider.OpenStorageManager())
            {
                var blockTxesStorage = storageManager.BlockTxesStorage;

                // create blocks
                var fakeBlock0 = CreateFakeBlock();
                var fakeBlock1 = CreateFakeBlock();
                var fakeBlock2 = CreateFakeBlock();

                // verify initial count of 0
                Assert.AreEqual(0, blockTxesStorage.BlockCount);

                // add blocks and verify count

                // 0
                blockTxesStorage.TryAddBlockTransactions(fakeBlock0.Hash, fakeBlock0.Transactions);
                Assert.AreEqual(1, blockTxesStorage.BlockCount);

                // 1
                blockTxesStorage.TryAddBlockTransactions(fakeBlock1.Hash, fakeBlock1.Transactions);
                Assert.AreEqual(2, blockTxesStorage.BlockCount);

                // 2
                blockTxesStorage.TryAddBlockTransactions(fakeBlock2.Hash, fakeBlock2.Transactions);
                Assert.AreEqual(3, blockTxesStorage.BlockCount);

                // remove blocks and verify count

                // 0
                blockTxesStorage.TryRemoveBlockTransactions(fakeBlock0.Hash);
                Assert.AreEqual(2, blockTxesStorage.BlockCount);

                // 1
                blockTxesStorage.TryRemoveBlockTransactions(fakeBlock1.Hash);
                Assert.AreEqual(1, blockTxesStorage.BlockCount);

                // 2
                blockTxesStorage.TryRemoveBlockTransactions(fakeBlock2.Hash);
                Assert.AreEqual(0, blockTxesStorage.BlockCount);
            }
        }

        // IBlockTxesStorage.ContainsBlock
        private void TestContainsBlock(ITestStorageProvider provider)
        {
            using (var storageManager = provider.OpenStorageManager())
            {
                var blockTxesStorage = storageManager.BlockTxesStorage;

                // create a block
                var block = CreateFakeBlock();

                // block should not be present
                Assert.IsFalse(blockTxesStorage.ContainsBlock(block.Hash));

                // add the block
                blockTxesStorage.TryAddBlockTransactions(block.Hash, block.Transactions);

                // block should be present
                Assert.IsTrue(blockTxesStorage.ContainsBlock(block.Hash)); ;

                // remove the block
                blockTxesStorage.TryRemoveBlockTransactions(block.Hash);

                // block should not be present
                Assert.IsFalse(blockTxesStorage.ContainsBlock(block.Hash)); ;
            }
        }

        // IBlockTxesStorage.TryAddRemoveBlockTransactions
        // IBlockTxesStorage.TryRemoveRemoveBlockTransactions
        private void TestTryAddRemoveBlockTransactions(ITestStorageProvider provider)
        {
            using (var storageManager = provider.OpenStorageManager())
            {
                var blockTxesStorage = storageManager.BlockTxesStorage;

                // create a block
                var block = CreateFakeBlock();

                // verify block can be added
                Assert.IsTrue(blockTxesStorage.TryAddBlockTransactions(block.Hash, block.Transactions));
                Assert.IsTrue(blockTxesStorage.ContainsBlock(block.Hash));

                // verify block cannot be added again
                Assert.IsFalse(blockTxesStorage.TryAddBlockTransactions(block.Hash, block.Transactions));

                // remove the block
                Assert.IsTrue(blockTxesStorage.TryRemoveBlockTransactions(block.Hash));
                Assert.IsFalse(blockTxesStorage.ContainsBlock(block.Hash));

                // verify block cannot be removed again
                Assert.IsFalse(blockTxesStorage.TryRemoveBlockTransactions(block.Hash));

                // verify block can be added again, after being removed
                Assert.IsTrue(blockTxesStorage.TryAddBlockTransactions(block.Hash, block.Transactions));
                Assert.IsTrue(blockTxesStorage.ContainsBlock(block.Hash));

                // verify block can be removed again, after being added again
                Assert.IsTrue(blockTxesStorage.TryRemoveBlockTransactions(block.Hash));
                Assert.IsFalse(blockTxesStorage.ContainsBlock(block.Hash));
            }
        }

        // IBlockTxesStorage.TryGetTransaction
        private void TestTryGetTransaction(ITestStorageProvider provider)
        {
            using (var storageManager = provider.OpenStorageManager())
            {
                var blockTxesStorage = storageManager.BlockTxesStorage;

                // create a block
                var block = CreateFakeBlock();

                // add block transactions
                blockTxesStorage.TryAddBlockTransactions(block.Hash, block.Transactions);

                // verify missing transactions
                Transaction transaction;
                Assert.IsFalse(blockTxesStorage.TryGetTransaction(UInt256.Zero, 0, out transaction));
                Assert.IsFalse(blockTxesStorage.TryGetTransaction(block.Hash, -1, out transaction));
                Assert.IsFalse(blockTxesStorage.TryGetTransaction(block.Hash, block.Transactions.Length, out transaction));

                // verify transactions
                for (var txIndex = 0; txIndex < block.Transactions.Length; txIndex++)
                {
                    Assert.IsTrue(blockTxesStorage.TryGetTransaction(block.Hash, txIndex, out transaction));
                    Assert.AreEqual(block.Transactions[txIndex].Hash, transaction.Hash);
                    Assert.AreEqual(transaction.Hash, DataCalculator.CalculateTransactionHash(transaction));
                }
            }
        }

        // IBlockTxesStorage.ReadBlockTransactions
        private void TestReadBlockTransactions(ITestStorageProvider provider)
        {
            using (var storageManager = provider.OpenStorageManager())
            {
                var blockTxesStorage = storageManager.BlockTxesStorage;

                // create a block
                var expectedBlock = CreateFakeBlock();
                var expectedBlockTxHashes = expectedBlock.Transactions.Select(x => x.Hash).ToList();

                // add block transactions
                blockTxesStorage.TryAddBlockTransactions(expectedBlock.Hash, expectedBlock.Transactions);

                // retrieve block transactions
                IEnumerator<BlockTx> rawActualBlockTxes;
                Assert.IsTrue(blockTxesStorage.TryReadBlockTransactions(expectedBlock.Hash, out rawActualBlockTxes));
                var actualBlockTxes = rawActualBlockTxes.UsingAsEnumerable().ToList();
                var actualBlockTxHashes = actualBlockTxes.Select(x => x.Transaction.Hash).ToList();

                // verify all retrieved transactions match their hashes
                Assert.IsTrue(actualBlockTxes.All(x => x.Hash == DataCalculator.CalculateTransactionHash(x.Transaction)));

                // verify retrieved block transactions match stored block transactions
                CollectionAssert.AreEqual(expectedBlockTxHashes, actualBlockTxHashes);
            }
        }

        // IBlockTxesStorage.PruneElements
        private void TestPruneElements(ITestStorageProvider provider)
        {
            // run 4 iterations of pruning: no adjacent blocks, one adjacent block on either side, and two adjacent blocks
            for (var iteration = 0; iteration < 4; iteration++)
            {
                provider.TestCleanup();

                using (var kernel = new StandardKernel())
                using (var storageManager = provider.OpenStorageManager())
                {
                    // add logging module
                    kernel.Load(new ConsoleLoggingModule(LogLevel.Debug));

                    var blockTxesStorage = storageManager.BlockTxesStorage;

                    // create a block
                    var block = CreateFakeBlock();
                    var txCount = block.Transactions.Length;

                    // determine expected merkle root node when fully pruned
                    var expectedFinalDepth = (int)Math.Ceiling(Math.Log(txCount, 2));
                    var expectedFinalElement = new BlockTx(index: 0, depth: expectedFinalDepth, hash: block.Header.MerkleRoot, pruned: true, transaction: null);

                    // pick a random pruning order
                    var random = new Random();
                    var pruneOrderSource = Enumerable.Range(0, txCount).ToList();
                    var pruneOrder = new List<int>(txCount);
                    while (pruneOrderSource.Count > 0)
                    {
                        var randomIndex = random.Next(pruneOrderSource.Count);

                        pruneOrder.Add(pruneOrderSource[randomIndex]);
                        pruneOrderSource.RemoveAt(randomIndex);
                    }

                    // add preceding block
                    if (iteration == 1 || iteration == 3)
                        blockTxesStorage.TryAddBlockTransactions(new UInt256(block.Hash.ToBigInteger() - 1), block.Transactions);

                    // add the block to be pruned
                    blockTxesStorage.TryAddBlockTransactions(block.Hash, block.Transactions);

                    // add proceeding block
                    if (iteration == 2 || iteration == 3)
                        blockTxesStorage.TryAddBlockTransactions(new UInt256(block.Hash.ToBigInteger() + 1), block.Transactions);

                    // prune the block
                    var count = 0;
                    foreach (var pruneIndex in pruneOrder)
                    {
                        Debug.WriteLine(count++);

                        // prune a transaction
                        blockTxesStorage.PruneElements(new[] { new KeyValuePair<UInt256, IEnumerable<int>>(block.Hash, new[] { pruneIndex }) });

                        // read block transactions
                        IEnumerator<BlockTx> blockTxes;
                        Assert.IsTrue(blockTxesStorage.TryReadBlockTransactions(block.Hash, out blockTxes));

                        // verify block transactions, exception will be fired if invalid
                        MerkleTree.ReadMerkleTreeNodes(block.Header.MerkleRoot, blockTxes.UsingAsEnumerable()).Count();
                    }

                    // read fully pruned block and verify
                    IEnumerator<BlockTx> finalBlockTxes;
                    Assert.IsTrue(blockTxesStorage.TryReadBlockTransactions(block.Hash, out finalBlockTxes));
                    var finalNodes = MerkleTree.ReadMerkleTreeNodes(block.Header.MerkleRoot, finalBlockTxes.UsingAsEnumerable()).ToList();
                    Assert.AreEqual(1, finalNodes.Count);
                    Assert.AreEqual(expectedFinalElement, finalNodes[0]);

                    // verify preceding block was not affected
                    if (iteration == 1 || iteration == 3)
                    {
                        Assert.IsTrue(blockTxesStorage.TryReadBlockTransactions(new UInt256(block.Hash.ToBigInteger() - 1), out finalBlockTxes));

                        // verify block transactions, exception will be fired if invalid
                        Assert.AreEqual(block.Transactions.Length, MerkleTree.ReadMerkleTreeNodes(block.Header.MerkleRoot, finalBlockTxes.UsingAsEnumerable()).Count());
                    }

                    // verify proceeding block was not affected
                    if (iteration == 2 || iteration == 3)
                    {
                        Assert.IsTrue(blockTxesStorage.TryReadBlockTransactions(new UInt256(block.Hash.ToBigInteger() + 1), out finalBlockTxes));

                        // verify block transactions, exception will be fired if invalid
                        Assert.AreEqual(block.Transactions.Length, MerkleTree.ReadMerkleTreeNodes(block.Header.MerkleRoot, finalBlockTxes.UsingAsEnumerable()).Count());
                    }
                }
            }
        }

        // IBlockTxesStorage.Flush
        private void TestFlush(ITestStorageProvider provider)
        {
            using (var storageManager = provider.OpenStorageManager())
            {
                var blockTxesStorage = storageManager.BlockTxesStorage;

                Assert.Inconclusive("TODO");
            }
        }

        // IBlockTxesStorage.Defragment
        private void TestDefragment(ITestStorageProvider provider)
        {
            using (var storageManager = provider.OpenStorageManager())
            {
                var blockTxesStorage = storageManager.BlockTxesStorage;

                Assert.Inconclusive("TODO");
            }
        }

        private Block CreateFakeBlock()
        {
            var txCount = 100;
            var transactions = Enumerable.Range(0, txCount).Select(x => RandomData.RandomTransaction()).ToImmutableArray();
            var blockHeader = RandomData.RandomBlockHeader().With(MerkleRoot: MerkleTree.CalculateMerkleRoot(transactions), Bits: DataCalculator.TargetToBits(UnitTestRules.Target0));
            var block = new Block(blockHeader, transactions);

            return block;
        }
    }
}
