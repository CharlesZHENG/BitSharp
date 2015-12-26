using BitSharp.Common;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NLog;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace BitSharp.Core.Test.Storage
{
    [TestClass]
    public class StorageIntegrationTest : StorageProviderTest
    {
        [TestMethod]
        [Timeout(600000/*ms*/)]
        public void TestRollback()
        {
            RunTest(TestRollback);
        }

        private void TestRollback(ITestStorageProvider provider)
        {
            var logger = LogManager.CreateNullLogger();

            var blockProvider = new TestNet3BlockProvider();
            //TODO this should go to height 5500 so that it will fail if blocks txes aren't rolled back in reverse
            //TODO it makes the test run fairly slow
            var blocks = blockProvider.ReadBlocks().Take(1000).ToList();

            var genesisBlock = blocks[0];
            var genesisHeader = new ChainedHeader(genesisBlock.Header, height: 0, totalWork: 0);
            var genesisChain = Chain.CreateForGenesisBlock(genesisHeader);

            var rules = new Testnet3Rules()
            {
                IgnoreSignatures = true,
                IgnoreScriptErrors = true
            };

            using (var storageManager = provider.OpenStorageManager())
            using (var coreStorage = new CoreStorage(storageManager))
            using (var chainStateBuilder = new ChainStateBuilder(rules, coreStorage, storageManager))
            {
                // add blocks to storage
                coreStorage.AddGenesisBlock(ChainedHeader.CreateForGenesisBlock(blocks[0].Header));
                foreach (var block in blocks)
                    coreStorage.TryAddBlock(block);

                // store empty utxo hash
                var expectedUtxoHashes = new List<UInt256>();
                using (var chainState = chainStateBuilder.ToImmutable())
                    expectedUtxoHashes.Add(UtxoCommitment.ComputeHash(chainState));

                // calculate utxo forward and store its state at each step along the way
                for (var blockIndex = 0; blockIndex < blocks.Count; blockIndex++)
                {
                    Debug.WriteLine($"Adding: {blockIndex:N0}");

                    var block = blocks[blockIndex];
                    var chainedHeader = new ChainedHeader(block.Header, blockIndex, 0);

                    chainStateBuilder.AddBlockAsync(chainedHeader, block.Transactions.Select(
                        (tx, txIndex) => BlockTx.Create(txIndex, tx))).Wait();

                    using (var chainState = chainStateBuilder.ToImmutable())
                        expectedUtxoHashes.Add(UtxoCommitment.ComputeHash(chainState));
                }

                // verify the utxo state before rolling back
                //TODO verify the UTXO hash hard-coded here is correct
                //TODO 5500: 0e9da3d53272cda9ecb6037c411ebc3cd0b65b5c16698baba41665edb29b8eaf
                var expectedLastUtxoHash = UInt256.ParseHex("11cff0cfe071249a315b5c7505905629fa9dd19069bfef1f3e2029592fcc5210");
                Assert.AreEqual(expectedLastUtxoHash, expectedUtxoHashes.Last());
                expectedUtxoHashes.RemoveAt(expectedUtxoHashes.Count - 1);

                // roll utxo backwards and validate its state at each step along the way
                for (var blockIndex = blocks.Count - 1; blockIndex >= 0; blockIndex--)
                {
                    Debug.WriteLine($"Rolling back: {blockIndex:N0}");

                    var block = blocks[blockIndex];
                    var chainedHeader = new ChainedHeader(block.Header, blockIndex, 0);
                    var blockTxes = block.Transactions.Select((tx, txIndex) => BlockTx.Create(txIndex, tx));

                    chainStateBuilder.RollbackBlock(chainedHeader, blockTxes);

                    var expectedUtxoHash = expectedUtxoHashes.Last();
                    expectedUtxoHashes.RemoveAt(expectedUtxoHashes.Count - 1);

                    using (var chainState = chainStateBuilder.ToImmutable())
                        Assert.AreEqual(expectedUtxoHash, UtxoCommitment.ComputeHash(chainState));
                }

                // verify chain state was rolled all the way back
                Assert.AreEqual(-1, chainStateBuilder.Chain.Height);
                Assert.AreEqual(0, expectedUtxoHashes.Count);

                // calculate utxo forward again
                for (var blockIndex = 0; blockIndex < blocks.Count; blockIndex++)
                {
                    Debug.WriteLine($"Adding: {blockIndex:N0}");

                    var block = blocks[blockIndex];
                    var chainedHeader = new ChainedHeader(block.Header, blockIndex, 0);

                    chainStateBuilder.AddBlockAsync(chainedHeader, block.Transactions.Select(
                        (tx, txIndex) => BlockTx.Create(txIndex, tx))).Wait();
                }

                // verify final utxo state again
                using (var chainState = chainStateBuilder.ToImmutable())
                    Assert.AreEqual(expectedLastUtxoHash, UtxoCommitment.ComputeHash(chainState));
            }
        }
    }
}
