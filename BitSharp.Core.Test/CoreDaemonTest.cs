using BitSharp.Common.ExtensionMethods;
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
                var block1 = daemon.MineAndAddEmptyBlock(daemon.GenesisBlock);

                daemon.WaitForUpdate();
                AssertMethods.AssertDaemonAtBlock(1, block1.Hash, daemon.CoreDaemon);
            }
        }

        [TestMethod]
        [Timeout(300000/*ms*/)]
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
                    block = daemon.MineAndAddEmptyBlock(block);
                }

                daemon.WaitForUpdate();
                AssertMethods.AssertDaemonAtBlock(height, block.Hash, daemon.CoreDaemon);
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
                var block1 = daemon.MineAndAddEmptyBlock(daemon.GenesisBlock);
                var block2 = daemon.MineAndAddEmptyBlock(block1);

                // check
                daemon.WaitForUpdate();
                AssertMethods.AssertDaemonAtBlock(2, block2.Hash, daemon.CoreDaemon);

                // attempt to spend block 2's coinbase in block 3
                var spendTx = daemon.TxManager.CreateSpendTransaction(block2.Transactions[0], 0, (byte)ScriptHashType.SIGHASH_ALL, 50 * SATOSHI_PER_BTC, daemon.CoinbasePrivateKey, daemon.CoinbasePublicKey, toPublicKey);
                var block3Unmined = daemon.CreateEmptyBlock(block2)
                    .WithAddedTransactions(spendTx);
                var block3 = daemon.MineAndAddBlock(block3Unmined);

                // check
                daemon.WaitForUpdate();
                AssertMethods.AssertDaemonAtBlock(3, block3.Hash, daemon.CoreDaemon);

                // add a simple block
                var block4 = daemon.MineAndAddEmptyBlock(block3);

                // check
                daemon.WaitForUpdate();
                AssertMethods.AssertDaemonAtBlock(4, block4.Hash, daemon.CoreDaemon);
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
                var block1 = daemon.MineAndAddEmptyBlock(daemon.GenesisBlock);
                var block2 = daemon.MineAndAddEmptyBlock(block1);

                // check
                daemon.WaitForUpdate();
                AssertMethods.AssertDaemonAtBlock(2, block2.Hash, daemon.CoreDaemon);

                // spend block 2's coinbase in block 3
                var spendTx = daemon.TxManager.CreateSpendTransaction(block2.Transactions[0], 0, (byte)ScriptHashType.SIGHASH_ALL, 50 * SATOSHI_PER_BTC, daemon.CoinbasePrivateKey, daemon.CoinbasePublicKey, toPublicKey);
                var block3Unmined = daemon.CreateEmptyBlock(block2)
                    .WithAddedTransactions(spendTx);
                var block3 = daemon.MineAndAddBlock(block3Unmined);

                // check
                daemon.WaitForUpdate();
                AssertMethods.AssertDaemonAtBlock(3, block3.Hash, daemon.CoreDaemon);

                // attempt to spend block 2's coinbase again in block 4
                var doubleSpendTx = daemon.TxManager.CreateSpendTransaction(block2.Transactions[0], 0, (byte)ScriptHashType.SIGHASH_ALL, 50 * SATOSHI_PER_BTC, daemon.CoinbasePrivateKey, daemon.CoinbasePublicKey, toPublicKeyBad);
                var block4BadUmined = daemon.CreateEmptyBlock(block3)
                    .WithAddedTransactions(doubleSpendTx);
                var block4Bad = daemon.MineAndAddBlock(block4BadUmined);

                // check that bad block wasn't added
                daemon.WaitForUpdate();
                AssertMethods.AssertDaemonAtBlock(3, block3.Hash, daemon.CoreDaemon);

                // add a simple block
                var block4Good = daemon.MineAndAddEmptyBlock(block3);

                // check
                daemon.WaitForUpdate();
                AssertMethods.AssertDaemonAtBlock(4, block4Good.Hash, daemon.CoreDaemon);
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
                var block1 = daemon.MineAndAddEmptyBlock(daemon.GenesisBlock);
                var block2 = daemon.MineAndAddEmptyBlock(block1);

                // check
                daemon.WaitForUpdate();
                AssertMethods.AssertDaemonAtBlock(2, block2.Hash, daemon.CoreDaemon);

                // attempt to spend block 2's coinbase in block 3, using twice its value
                var spendTx = daemon.TxManager.CreateSpendTransaction(block2.Transactions[0], 0, (byte)ScriptHashType.SIGHASH_ALL, 100 * SATOSHI_PER_BTC, daemon.CoinbasePrivateKey, daemon.CoinbasePublicKey, toPublicKey);
                var block3BadUnmined = daemon.CreateEmptyBlock(block2)
                    .WithAddedTransactions(spendTx);
                var block3Bad = daemon.MineAndAddBlock(block3BadUnmined);

                // check that bad block wasn't added
                daemon.WaitForUpdate();
                AssertMethods.AssertDaemonAtBlock(2, block2.Hash, daemon.CoreDaemon);

                // add a simple block
                var block3Good = daemon.MineAndAddEmptyBlock(block2);

                // check
                daemon.WaitForUpdate();
                AssertMethods.AssertDaemonAtBlock(3, block3Good.Hash, daemon.CoreDaemon);
            }
        }

        [TestMethod]
        public void TestSimpleBlockchainSplit()
        {
            using (var daemon1 = new TestDaemon())
            {
                // add some simple blocks
                var block1 = daemon1.MineAndAddEmptyBlock(daemon1.GenesisBlock);
                var block2 = daemon1.MineAndAddEmptyBlock(block1);
                var block3a = daemon1.MineAndAddEmptyBlock(block2);
                daemon1.WaitForUpdate();

                // wait for daemon
                daemon1.WaitForUpdate();
                AssertMethods.AssertDaemonAtBlock(3, block3a.Hash, daemon1.CoreDaemon);

                // introduce a tie split
                var block3b = daemon1.MineAndAddEmptyBlock(block2);

                // check that 3a is still current as it was first
                daemon1.WaitForUpdate();
                AssertMethods.AssertDaemonAtBlock(3, block3a.Hash, daemon1.CoreDaemon);

                // continue with currently winning branch
                var block4a = daemon1.MineAndAddEmptyBlock(block3a);

                // wait for daemon
                daemon1.WaitForUpdate();
                AssertMethods.AssertDaemonAtBlock(4, block4a.Hash, daemon1.CoreDaemon);

                // continue with tied branch
                var block4b = daemon1.MineAndAddEmptyBlock(block3b);

                // check that 4a is still current as it was first
                daemon1.WaitForUpdate();
                AssertMethods.AssertDaemonAtBlock(4, block4a.Hash, daemon1.CoreDaemon);

                // resolve tie split, with other chain winning
                var block5b = daemon1.MineAndAddEmptyBlock(block4b);

                // check that blockchain reorged to the winning chain
                daemon1.WaitForUpdate();
                AssertMethods.AssertDaemonAtBlock(5, block5b.Hash, daemon1.CoreDaemon);

                // continue on winning fork
                var block6b = daemon1.MineAndAddEmptyBlock(block5b);

                // check that blockchain continued on the winning chain
                daemon1.WaitForUpdate();
                AssertMethods.AssertDaemonAtBlock(6, block6b.Hash, daemon1.CoreDaemon);

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
                    AssertMethods.AssertDaemonAtBlock(6, block6b.Hash, daemon2.CoreDaemon);

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

        [TestMethod]
        public void TestShorterChainWins()
        {
            using (var daemon = new TestDaemon())
            {
                // add some simple blocks
                var block1 = daemon.MineAndAddEmptyBlock(daemon.GenesisBlock);
                var block2 = daemon.MineAndAddEmptyBlock(block1);
                var block3a = daemon.MineAndAddEmptyBlock(block2);
                var block4a = daemon.MineAndAddEmptyBlock(block3a);
                var block5a = daemon.MineAndAddEmptyBlock(block4a);

                // check
                daemon.WaitForUpdate();
                AssertMethods.AssertDaemonAtBlock(5, block5a.Hash, daemon.CoreDaemon);

                // create a split with 3b, but do more work than current height 5 chain
                daemon.Rules.SetHighestTarget(UnitTestRules.Target2);
                var block3b = daemon.MineAndAddEmptyBlock(block2, UnitTestRules.Target2);

                // check that blockchain reorganized to shorter chain
                daemon.WaitForUpdate();
                AssertMethods.AssertDaemonAtBlock(3, block3b.Hash, daemon.CoreDaemon);
            }
        }
    }
}
