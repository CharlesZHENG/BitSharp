using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NLog;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace BitSharp.Core.Test.Storage
{
    [TestClass]
    public class StorageIntegrationTest : StorageProviderTest
    {
        [TestMethod]
        [Timeout(10 * /*minutes*/(60 * 1000))]
        public void TestRollback()
        {
            RunTest(TestRollback);
        }

        private void TestRollback(ITestStorageProvider provider)
        {
            ConsoleLoggingModule.Configure();
            var logger = LogManager.GetCurrentClassLogger();

            var blockCount = 10.THOUSAND();
            var checkUtxoHashFrequencey = 1000;

            var blockProvider = new TestNet3BlockProvider();
            var blocks = blockProvider.ReadBlocks().Take(blockCount).ToList();

            var genesisBlock = blocks[0];
            var genesisHeader = new ChainedHeader(genesisBlock.Header, height: 0, totalWork: 0, dateSeen: DateTimeOffset.Now);
            var genesisChain = Chain.CreateForGenesisBlock(genesisHeader);

            var chainParams = new Testnet3Params();
            var rules = new CoreRules(chainParams)
            {
                IgnoreScripts = true,
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
                    logger.Info($"Adding: {blockIndex:N0}");

                    var block = blocks[blockIndex];
                    var chainedHeader = new ChainedHeader(block.Header, blockIndex, 0, DateTimeOffset.Now);

                    chainStateBuilder.AddBlockAsync(chainedHeader, block.Transactions.Select(
                        (tx, txIndex) => BlockTx.Create(txIndex, tx))).Wait();

                    if (blockIndex % checkUtxoHashFrequencey == 0 || blockIndex == blocks.Count - 1)
                        using (var chainState = chainStateBuilder.ToImmutable())
                            expectedUtxoHashes.Add(UtxoCommitment.ComputeHash(chainState));
                }

                // verify the utxo state before rolling back
                var expectedLastUtxoHash = UInt256.ParseHex("5f155c7d8a5c850d5fb2566aec5110caa40e270184126d17022ae9780fd65fd9");
                Assert.AreEqual(expectedLastUtxoHash, expectedUtxoHashes.Last());
                expectedUtxoHashes.RemoveAt(expectedUtxoHashes.Count - 1);

                // roll utxo backwards and validate its state at each step along the way
                for (var blockIndex = blocks.Count - 1; blockIndex >= 0; blockIndex--)
                {
                    logger.Info($"Rolling back: {blockIndex:N0}");

                    var block = blocks[blockIndex];
                    var chainedHeader = new ChainedHeader(block.Header, blockIndex, 0, DateTimeOffset.Now);
                    var blockTxes = block.Transactions.Select((tx, txIndex) => BlockTx.Create(txIndex, tx));

                    chainStateBuilder.RollbackBlock(chainedHeader, blockTxes);

                    if ((blockIndex - 1) % checkUtxoHashFrequencey == 0 || blockIndex == 0)
                    {
                        var expectedUtxoHash = expectedUtxoHashes.Last();
                        expectedUtxoHashes.RemoveAt(expectedUtxoHashes.Count - 1);

                        using (var chainState = chainStateBuilder.ToImmutable())
                            Assert.AreEqual(expectedUtxoHash, UtxoCommitment.ComputeHash(chainState));
                    }
                }

                // verify chain state was rolled all the way back
                Assert.AreEqual(-1, chainStateBuilder.Chain.Height);
                Assert.AreEqual(0, expectedUtxoHashes.Count);

                // calculate utxo forward again
                for (var blockIndex = 0; blockIndex < blocks.Count; blockIndex++)
                {
                    logger.Info($"Adding: {blockIndex:N0}");

                    var block = blocks[blockIndex];
                    var chainedHeader = new ChainedHeader(block.Header, blockIndex, 0, DateTimeOffset.Now);

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
