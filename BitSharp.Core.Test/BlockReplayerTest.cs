using BitSharp.Common.Test;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using BitSharp.Core.Script;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NLog;
using System.Collections.Generic;
using System.Linq;

namespace BitSharp.Core.Test
{
    [TestClass]
    public class BlockReplayerTest
    {
        [TestMethod]
        [Timeout(5 * /*minutes*/(60 * 1000))]
        public void TestReplayBlock()
        {
            var logger = LogManager.CreateNullLogger();

            using (var simulator = new MainnetSimulator())
            {
                simulator.AddBlockRange(0, 9999);
                simulator.WaitForUpdate();

                using (var chainState = simulator.CoreDaemon.GetChainState())
                {
                    Assert.AreEqual(9999, chainState.Chain.Height);

                    for (var blockHeight = 0; blockHeight <= chainState.Chain.Height; blockHeight++)
                    {
                        var blockHash = chainState.Chain.Blocks[blockHeight].Hash;

                        var expectedTransactions = simulator.BlockProvider.GetBlock(blockHeight).Transactions;
                        var actualTransactions = BlockReplayer.ReplayBlock(simulator.CoreDaemon.CoreStorage, chainState, blockHash, replayForward: true)
                            .ToEnumerable().ToList();

                        CollectionAssert.AreEqual(
                            expectedTransactions.Select(x => x.Hash).ToList(),
                            actualTransactions.Select(x => x.Transaction.Hash).ToList(),
                            $"Transactions differ at block {blockHeight:N0}");
                    }
                }
            }
        }

        // this test replays a rollback 2 blocks deep (creates blocks 0-3, 2-3 are rolled back)
        // block 3 spends a transaction in block 2
        // because block 2 is rolled back, the UnspentTx information that block 3 needs will be removed entirely from the chain state
        //
        // this test verifies the information needed to replay a rolled-back block
        [TestMethod]
        public void TestReplayBlockRollback()
        {
            var logger = LogManager.CreateNullLogger();

            using (var daemon = new TestDaemon())
            {
                // create a new keypair to spend to
                var toKeyPair = daemon.TxManager.CreateKeyPair();
                var toPrivateKey = toKeyPair.Item1;
                var toPublicKey = toKeyPair.Item2;

                // add block 1
                var block0 = daemon.GenesisBlock;
                var block1 = daemon.MineAndAddEmptyBlock();

                // add some blocks so coinbase is mature to spend
                Block lastBlock = null;
                for (var i = 0; i < 100; i++)
                    lastBlock = daemon.MineAndAddEmptyBlock();

                // add block 2, spending from block 1
                var spendTx1 = daemon.TxManager.CreateSpendTransaction(block1.Transactions[0], 0, (byte)ScriptHashType.SIGHASH_ALL, block1.Transactions[0].OutputValue(), daemon.CoinbasePrivateKey, daemon.CoinbasePublicKey, toPublicKey);
                var block2Unmined = daemon.CreateEmptyBlock(lastBlock.Hash)
                    .CreateWithAddedTransactions(spendTx1);
                var block2 = daemon.MineAndAddBlock(block2Unmined);

                // add some blocks so coinbase is mature to spend
                for (var i = 0; i < 100; i++)
                    lastBlock = daemon.MineAndAddEmptyBlock();

                // add block 3, spending from block 2
                var spendTx2 = daemon.TxManager.CreateSpendTransaction(block2.Transactions[1], 0, (byte)ScriptHashType.SIGHASH_ALL, block2.Transactions[1].OutputValue(), toPrivateKey, toPublicKey, toPublicKey);
                var block3Unmined = daemon.CreateEmptyBlock(lastBlock.Hash)
                    .CreateWithAddedTransactions(spendTx2);
                var block3 = daemon.MineAndAddBlock(block3Unmined);

                // replay all blocks up to block 3
                daemon.WaitForUpdate();
                using (var chainState = daemon.CoreDaemon.GetChainState())
                {
                    Assert.AreEqual(203, chainState.Chain.Height);

                    var replayTransactions = new List<ValidatableTx>();
                    foreach (var blockHash in chainState.Chain.Blocks.Select(x => x.Hash))
                    {
                        replayTransactions.AddRange(BlockReplayer.ReplayBlock(daemon.CoreStorage, chainState, blockHash, replayForward: true)
                            .ToEnumerable());
                    }

                    // verify all transactions were replayed
                    Assert.AreEqual(206, replayTransactions.Count);
                    Assert.AreEqual(block0.Transactions[0].Hash, replayTransactions[0].Transaction.Hash);
                    Assert.AreEqual(block1.Transactions[0].Hash, replayTransactions[1].Transaction.Hash);
                    Assert.AreEqual(block2.Transactions[0].Hash, replayTransactions[102].Transaction.Hash);
                    Assert.AreEqual(block2.Transactions[1].Hash, replayTransactions[103].Transaction.Hash);
                    Assert.AreEqual(block3.Transactions[0].Hash, replayTransactions[204].Transaction.Hash);
                    Assert.AreEqual(block3.Transactions[1].Hash, replayTransactions[205].Transaction.Hash);
                }

                // mark block 2 invalid, it will be rolled back
                daemon.CoreStorage.MarkBlockInvalid(block2.Hash, daemon.CoreDaemon.TargetChain);
                daemon.WaitForUpdate();

                // replay rollback of block 3
                using (var chainState = daemon.CoreDaemon.GetChainState())
                {
                    Assert.AreEqual(101, chainState.Chain.Height);

                    var replayTransactions = new List<ValidatableTx>(
                        BlockReplayer.ReplayBlock(daemon.CoreStorage, chainState, block3.Hash, replayForward: false)
                        .ToEnumerable());

                    // verify transactions were replayed
                    Assert.AreEqual(2, replayTransactions.Count);
                    Assert.AreEqual(block3.Transactions[1].Hash, replayTransactions[0].Transaction.Hash);
                    Assert.AreEqual(block3.Transactions[0].Hash, replayTransactions[1].Transaction.Hash);

                    // verify correct previous output was replayed (block 3 tx 1 spent block 2 tx 1)
                    Assert.AreEqual(1, replayTransactions[0].PrevTxOutputs.Length);
                    CollectionAssert.AreEqual(block2.Transactions[1].Outputs[0].ScriptPublicKey, replayTransactions[0].PrevTxOutputs[0].ScriptPublicKey);

                    // verify correct previous output was replayed (block 3 tx 0 spends nothing, coinbase)
                    Assert.AreEqual(0, replayTransactions[1].PrevTxOutputs.Length);
                }

                // replay rollback of block 2
                using (var chainState = daemon.CoreDaemon.GetChainState())
                {
                    Assert.AreEqual(101, chainState.Chain.Height);

                    var replayTransactions = new List<ValidatableTx>(
                        BlockReplayer.ReplayBlock(daemon.CoreStorage, chainState, block2.Hash, replayForward: false)
                        .ToEnumerable());

                    // verify transactions were replayed
                    Assert.AreEqual(2, replayTransactions.Count);
                    Assert.AreEqual(block2.Transactions[1].Hash, replayTransactions[0].Transaction.Hash);
                    Assert.AreEqual(block2.Transactions[0].Hash, replayTransactions[1].Transaction.Hash);

                    // verify correct previous output was replayed (block 2 tx 1 spent block 1 tx 0)
                    Assert.AreEqual(1, replayTransactions[0].PrevTxOutputs.Length);
                    CollectionAssert.AreEqual(block1.Transactions[0].Outputs[0].ScriptPublicKey, replayTransactions[0].PrevTxOutputs[0].ScriptPublicKey);

                    // verify correct previous output was replayed (block 2 tx 0 spends nothing, coinbase)
                    Assert.AreEqual(0, replayTransactions[1].PrevTxOutputs.Length);
                }
            }
        }
    }
}
