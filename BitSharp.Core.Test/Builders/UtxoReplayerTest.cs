using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using System.Collections.Generic;
using System.Linq;

namespace BitSharp.Core.Test.Builders
{
    [TestClass]
    public class UtxoReplayerTest
    {
        [TestMethod]
        public void TestReplayForward()
        {
            var coreStorage = new Mock<ICoreStorage>();
            var chainState = new Mock<IChainState>();

            var testBlocks = new TestBlocks();

            var block = testBlocks.MineAndAddBlock(txCount: 10);
            var chainedHeader = testBlocks.Chain.LastBlock;

            chainState.Setup(x => x.Chain).Returns(() => testBlocks.Chain);

            // mock block txes read
            var blockTxes = block.Transactions.Select((tx, txIndex) => new BlockTx(txIndex, tx)).GetEnumerator();
            coreStorage.Setup(x => x.TryReadBlockTransactions(chainedHeader.Hash, true, out blockTxes)).Returns(true);

            // mock unspent tx lookup
            for (var txIndex = 0; txIndex < block.Transactions.Length; txIndex++)
            {
                var tx = block.Transactions[txIndex];
                for (var inputIndex = 0; inputIndex < tx.Inputs.Length; inputIndex++)
                {
                    var input = tx.Inputs[inputIndex];

                    // create a fake unspent tx, with enough outputs for this input
                    var unspentTx = new UnspentTx(input.PreviousTxOutputKey.TxHash, blockIndex: 1, txIndex: txIndex * inputIndex,
                        outputStates: new OutputStates(input.PreviousTxOutputKey.TxOutputIndex.ToIntChecked() + 1, OutputState.Unspent));

                    chainState.Setup(x => x.TryGetUnspentTx(unspentTx.TxHash, out unspentTx)).Returns(true);
                }
            }

            var loadingTxes = UtxoReplayer.ReplayCalculateUtxo(coreStorage.Object, chainState.Object, chainedHeader);
            using (var loadingTxesQueue = loadingTxes.LinkToQueue())
            {
                var txIndex = -1;
                foreach (var loadingTx in loadingTxesQueue.GetConsumingEnumerable())
                {
                    txIndex++;
                    
                    // verify loading tx matches original block tx
                    Assert.AreEqual(block.Transactions[txIndex].Hash, loadingTx.Transaction.Hash);

                    // if coinbase, verify no lookup keys for coinbase inputs
                    if (txIndex == 0)
                    {
                        Assert.AreEqual(0, loadingTx.PrevOutputTxKeys.Length);
                    }
                    else
                    {
                        // verify there is a lookup key for each input
                        Assert.AreEqual(block.Transactions[txIndex].Inputs.Length, loadingTx.PrevOutputTxKeys.Length);

                        // verify each lookup key matches the mocked data
                        for (var inputIndex = 0; inputIndex < loadingTx.Transaction.Inputs.Length; inputIndex++)
                        {
                            var prevOutputTxKey = loadingTx.PrevOutputTxKeys[inputIndex];
                            Assert.AreEqual(prevOutputTxKey.BlockHash, block.Hash);
                            Assert.AreEqual(prevOutputTxKey.TxIndex, txIndex * inputIndex);
                        }
                    }
                }

                // verify correct number of transactions were replayed
                Assert.AreEqual(txIndex + 1, block.Transactions.Length);
            }
        }
    }
}
