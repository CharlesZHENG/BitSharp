using BitSharp.Common.ExtensionMethods;
using BitSharp.Common.Test;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
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
            chainState.Setup(x => x.CursorCount).Returns(4);

            var testBlocks = new TestBlocks();

            var block = testBlocks.MineAndAddBlock(txCount: 10);
            var chainedHeader = testBlocks.Chain.LastBlock;

            chainState.Setup(x => x.Chain).Returns(() => testBlocks.Chain);

            // mock block txes read
            var blockTxes = block.Transactions.Select((tx, txIndex) => (BlockTx)BlockTx.Create(txIndex, tx)).GetEnumerator();
            coreStorage.Setup(x => x.TryReadBlockTransactions(chainedHeader.Hash, out blockTxes)).Returns(true);

            // mock unspent tx lookup
            var expectedValue = 50UL * (ulong)100.MILLION();
            for (var txIndex = 0; txIndex < block.Transactions.Length; txIndex++)
            {
                var tx = block.Transactions[txIndex];
                for (var inputIndex = 0; inputIndex < tx.Inputs.Length; inputIndex++)
                {
                    var input = tx.Inputs[inputIndex];

                    // create a fake unspent tx, with enough outputs for this input
                    var unspentTx = new UnspentTx(input.PrevTxOutputKey.TxHash, blockIndex: 1, txIndex: txIndex * inputIndex, txVersion: 0, isCoinbase: false,
                        outputStates: tx.IsCoinbase ? OutputStates.Empty : new OutputStates(input.PrevTxOutputKey.TxOutputIndex.ToIntChecked() + 1, OutputState.Unspent));
                    var txOutput = new TxOutput(tx.Outputs[0].Value, tx.Outputs[0].ScriptPublicKey);

                    chainState.Setup(x => x.TryGetUnspentTx(unspentTx.TxHash, out unspentTx)).Returns(true);
                    chainState.Setup(x => x.TryGetUnspentTxOutput(input.PrevTxOutputKey, out txOutput)).Returns(true);
                }
            }

            var validatableTxes = UtxoReplayer.ReplayCalculateUtxo(coreStorage.Object, chainState.Object, chainedHeader).ToEnumerable().ToList();

            // verify correct number of transactions were replayed
            Assert.AreEqual(validatableTxes.Count, block.Transactions.Length);

            expectedValue = 50UL * (ulong)100.MILLION();
            foreach (var validatableTx in validatableTxes)
            {
                // verify validatable tx matches original block tx
                Assert.AreEqual(block.Transactions[validatableTx.Index].Hash, validatableTx.Transaction.Hash);

                // if coinbase, verify no tx outputs for coinbase inputs
                if (validatableTx.IsCoinbase)
                {
                    Assert.AreEqual(0, validatableTx.PrevTxOutputs.Length);
                }
                else
                {
                    // verify there is a tx output for each input
                    Assert.AreEqual(block.Transactions[validatableTx.Index].Inputs.Length, validatableTx.PrevTxOutputs.Length);

                    // verify each tx output matches the mocked data
                    for (var inputIndex = 0; inputIndex < validatableTx.Transaction.Inputs.Length; inputIndex++)
                    {
                        var prevTxOutput = validatableTx.PrevTxOutputs[inputIndex];

                        expectedValue -= 1;
                        Assert.AreEqual(expectedValue, prevTxOutput.Value);
                        CollectionAssert.AreEqual(block.Transactions[0].Outputs[0].ScriptPublicKey, prevTxOutput.ScriptPublicKey);
                    }
                }
            }
        }
    }
}
