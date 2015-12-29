using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Script;
using BitSharp.Core.Test.Rules;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;

namespace BitSharp.Core.Test
{
    [TestClass]
    public class CoreDaemonTest
    {
        private const UInt64 SATOSHI_PER_BTC = 100 * 1000 * 1000;

        private readonly Random random = new Random();

        [TestMethod]
        public void TestAddSingleBlock()
        {
            using (var daemon = new TestDaemon())
            {
                var block1 = daemon.MineAndAddEmptyBlock();

                daemon.WaitForUpdate();
                daemon.AssertAtBlock(1, block1.Hash);
            }
        }

        [TestMethod]
        [Timeout(5 * /*minutes*/(60 * 1000))]
        public void TestLongBlockchain()
        {
            using (var daemon = new TestDaemon())
            {
                var count = 1.THOUSAND();

                var height = 0;
                var block = daemon.GenesisBlock;
                for (var i = 0; i < count; i++)
                {
                    height++;
                    block = daemon.MineAndAddEmptyBlock();
                }

                daemon.WaitForUpdate();
                daemon.AssertAtBlock(height, block.Hash);
            }
        }

        [TestMethod]
        public void TestSimpleSpend()
        {
            using (var daemon = new TestDaemon())
            {
                // create a new keypair to spend to
                var toKeyPair = daemon.TxManager.CreateKeyPair();
                var toPrivateKey = toKeyPair.Item1;
                var toPublicKey = toKeyPair.Item2;

                // add some simple blocks
                var block1 = daemon.MineAndAddEmptyBlock();
                var block2 = daemon.MineAndAddEmptyBlock();

                // add some blocks so coinbase is mature to spend
                Block lastBlock = null;
                for (var i = 0; i < 100; i++)
                    lastBlock = daemon.MineAndAddEmptyBlock();

                // check
                daemon.WaitForUpdate();
                daemon.AssertAtBlock(102, lastBlock.Hash);

                // attempt to spend block 2's coinbase in block 3
                var spendTx = daemon.TxManager.CreateSpendTransaction(block2.Transactions[0], 0, (byte)ScriptHashType.SIGHASH_ALL, 50 * SATOSHI_PER_BTC, daemon.CoinbasePrivateKey, daemon.CoinbasePublicKey, toPublicKey);
                var block3Unmined = daemon.CreateEmptyBlock(lastBlock.Hash)
                    .CreateWithAddedTransactions(spendTx);
                var block3 = daemon.MineAndAddBlock(block3Unmined);

                // check
                daemon.WaitForUpdate();
                daemon.AssertAtBlock(103, block3.Hash);

                // add a simple block
                var block4 = daemon.MineAndAddEmptyBlock();

                // check
                daemon.WaitForUpdate();
                daemon.AssertAtBlock(104, block4.Hash);
            }
        }

        [TestMethod]
        public void TestDoubleSpend()
        {
            using (var daemon = new TestDaemon())
            {
                // create a new keypair to spend to
                var toKeyPair = daemon.TxManager.CreateKeyPair();
                var toPrivateKey = toKeyPair.Item1;
                var toPublicKey = toKeyPair.Item2;

                // create a new keypair to double spend to
                var toKeyPairBad = daemon.TxManager.CreateKeyPair();
                var toPrivateKeyBad = toKeyPair.Item1;
                var toPublicKeyBad = toKeyPair.Item2;

                // add some simple blocks
                var block1 = daemon.MineAndAddEmptyBlock();
                var block2 = daemon.MineAndAddEmptyBlock();

                // add some blocks so coinbase is mature to spend
                Block lastBlock = null;
                for (var i = 0; i < 100; i++)
                    lastBlock = daemon.MineAndAddEmptyBlock();

                // check
                daemon.WaitForUpdate();
                daemon.AssertAtBlock(102, lastBlock.Hash);

                // spend block 2's coinbase in block 3
                var spendTx = daemon.TxManager.CreateSpendTransaction(block2.Transactions[0], 0, (byte)ScriptHashType.SIGHASH_ALL, 50 * SATOSHI_PER_BTC, daemon.CoinbasePrivateKey, daemon.CoinbasePublicKey, toPublicKey);
                var block3Unmined = daemon.CreateEmptyBlock(lastBlock.Hash)
                    .CreateWithAddedTransactions(spendTx);
                var block3 = daemon.MineAndAddBlock(block3Unmined);

                // check
                daemon.WaitForUpdate();
                daemon.AssertAtBlock(103, block3.Hash);

                // attempt to spend block 2's coinbase again in block 4
                var doubleSpendTx = daemon.TxManager.CreateSpendTransaction(block2.Transactions[0], 0, (byte)ScriptHashType.SIGHASH_ALL, 50 * SATOSHI_PER_BTC, daemon.CoinbasePrivateKey, daemon.CoinbasePublicKey, toPublicKeyBad);
                var block4BadUmined = daemon.CreateEmptyBlock(block3.Hash)
                    .CreateWithAddedTransactions(doubleSpendTx);
                var block4Bad = daemon.MineAndAddBlock(block4BadUmined);

                // check that bad block wasn't added
                daemon.WaitForUpdate();
                daemon.AssertAtBlock(103, block3.Hash);

                // add a simple block
                daemon.TestBlocks.Rollback(1);
                var block4Good = daemon.MineAndAddEmptyBlock();

                // check
                daemon.WaitForUpdate();
                daemon.AssertAtBlock(104, block4Good.Hash);
            }
        }

        [TestMethod]
        public void TestInsufficientBalance()
        {
            using (var daemon = new TestDaemon())
            {
                // create a new keypair to spend to
                var toKeyPair = daemon.TxManager.CreateKeyPair();
                var toPrivateKey = toKeyPair.Item1;
                var toPublicKey = toKeyPair.Item2;

                // add some simple blocks
                var block1 = daemon.MineAndAddEmptyBlock();
                var block2 = daemon.MineAndAddEmptyBlock();

                // check
                daemon.WaitForUpdate();
                daemon.AssertAtBlock(2, block2.Hash);

                // attempt to spend block 2's coinbase in block 3, using twice its value
                var spendTx = daemon.TxManager.CreateSpendTransaction(block2.Transactions[0], 0, (byte)ScriptHashType.SIGHASH_ALL, 100 * SATOSHI_PER_BTC, daemon.CoinbasePrivateKey, daemon.CoinbasePublicKey, toPublicKey);
                var block3BadUnmined = daemon.CreateEmptyBlock(block2.Hash)
                    .CreateWithAddedTransactions(spendTx);
                var block3Bad = daemon.MineAndAddBlock(block3BadUnmined);

                // check that bad block wasn't added
                daemon.WaitForUpdate();
                daemon.AssertAtBlock(2, block2.Hash);

                // add a simple block
                daemon.TestBlocks.Rollback(1);
                var block3Good = daemon.MineAndAddEmptyBlock();

                // check
                daemon.WaitForUpdate();
                daemon.AssertAtBlock(3, block3Good.Hash);
            }
        }

        [TestMethod]
        public void TestSimpleBlockchainSplit()
        {
            using (var daemon1 = new TestDaemon())
            {
                // add some simple blocks
                var block1 = daemon1.MineAndAddEmptyBlock();
                var block2 = daemon1.MineAndAddEmptyBlock();
                var block3a = daemon1.MineAndAddEmptyBlock();
                daemon1.WaitForUpdate();

                // create a fork test block chain, starting at block2
                var testBlocksFork = daemon1.TestBlocks.Fork(1);

                // wait for daemon
                daemon1.WaitForUpdate();
                daemon1.AssertAtBlock(3, block3a.Hash);

                // introduce a tie split
                var block3b = daemon1.AddBlock(testBlocksFork.MineAndAddEmptyBlock());

                // check that 3a is still current as it was first
                daemon1.WaitForUpdate();
                daemon1.AssertAtBlock(3, block3a.Hash);

                // continue with currently winning branch
                var block4a = daemon1.MineAndAddEmptyBlock();

                // wait for daemon
                daemon1.WaitForUpdate();
                daemon1.AssertAtBlock(4, block4a.Hash);

                // continue with tied branch
                var block4b = daemon1.AddBlock(testBlocksFork.MineAndAddEmptyBlock());

                // check that 4a is still current as it was first
                daemon1.WaitForUpdate();
                daemon1.AssertAtBlock(4, block4a.Hash);

                // resolve tie split, with other chain winning
                var block5b = daemon1.AddBlock(testBlocksFork.MineAndAddEmptyBlock());

                // check that blockchain reorged to the winning chain
                daemon1.WaitForUpdate();
                daemon1.AssertAtBlock(5, block5b.Hash);

                // continue on winning fork
                var block6b = daemon1.AddBlock(testBlocksFork.MineAndAddEmptyBlock());

                // check that blockchain continued on the winning chain
                daemon1.WaitForUpdate();
                daemon1.AssertAtBlock(6, block6b.Hash);

                // create a second blockchain, reusing the genesis from the first
                using (var daemon2 = new TestDaemon(daemon1.GenesisBlock))
                {
                    // add only the winning blocks to the second blockchain
                    daemon2.AddBlock(block1);
                    daemon2.AddBlock(block2);
                    daemon2.AddBlock(block3b);
                    daemon2.AddBlock(block4b);
                    daemon2.AddBlock(block5b);
                    daemon2.AddBlock(block6b);

                    // check second blockchain
                    daemon2.WaitForUpdate();
                    daemon2.AssertAtBlock(6, block6b.Hash);

                    // verify that re-organized blockchain matches winning-only blockchain
                    using (var expectedChainState = daemon2.CoreDaemon.GetChainState())
                    using (var actualChainState = daemon1.CoreDaemon.GetChainState())
                    {
                        var expectedUtxo = expectedChainState.ReadUnspentTransactions().ToList();
                        var actualUtxo = actualChainState.ReadUnspentTransactions().ToList();

                        CollectionAssert.AreEqual(expectedUtxo, actualUtxo);
                    }
                }
            }
        }

        //TODO needs to be updated to not violate required target rules
        [Ignore]
        [TestMethod]
        public void TestShorterChainWins()
        {
            using (var daemon = new TestDaemon())
            {
                // add some simple blocks
                var block1 = daemon.MineAndAddEmptyBlock();
                var block2 = daemon.MineAndAddEmptyBlock();
                var block3a = daemon.MineAndAddEmptyBlock();
                var block4a = daemon.MineAndAddEmptyBlock();
                var block5a = daemon.MineAndAddEmptyBlock();

                // check
                daemon.WaitForUpdate();
                daemon.AssertAtBlock(5, block5a.Hash);

                // create a split with 3b, but do more work than current height 5 chain
                var testBlocksFork = daemon.TestBlocks.Fork(3);
                daemon.ChainParams.SetHighestTarget(UnitTestParams.Target2);
                var block3b = daemon.AddBlock(testBlocksFork.MineAndAddEmptyBlock(UnitTestParams.Target2));

                // check that blockchain reorganized to shorter chain
                daemon.WaitForUpdate();
                daemon.AssertAtBlock(3, block3b.Hash);
            }
        }
    }
}
