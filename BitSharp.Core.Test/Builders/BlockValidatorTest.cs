using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using System;
using System.Collections.Immutable;
using System.Threading.Tasks.Dataflow;

namespace BitSharp.Core.Test.Builders
{
    [TestClass]
    public class BlockValidatorTest
    {
        [TestMethod]
        public void TestInvalidMerkleRoot()
        {
            var coreStorage = Mock.Of<ICoreStorage>();

            var testBlocks = new TestBlocks();
            var rules = testBlocks.Rules;

            var block = testBlocks.MineAndAddBlock(txCount: 10);
            var chainedHeader = testBlocks.Chain.LastBlock;

            // create an invalid version of the header where the merkle root is incorrect
            var invalidChainedHeader = ChainedHeader.CreateFromPrev(rules.GenesisChainedHeader, block.Header.With(MerkleRoot: UInt256.Zero));

            // feed validator loaded txes
            var loadedTxes = new BufferBlock<LoadedTx>();
            var txIndex = -1;
            foreach (var tx in block.Transactions)
            {
                var inputTxes = new Transaction[tx.Inputs.Length];
                for (var inputIndex = 0; inputIndex < tx.Inputs.Length; inputIndex++)
                {
                    // create a transaction for each input, ensure it has an output available for this input
                    var inputTx = RandomData.RandomTransaction(new RandomDataOptions
                    {
                        TxOutputCount = tx.Inputs[inputIndex].PreviousTxOutputKey.TxOutputIndex.ToIntChecked() + 1
                    });
                    inputTxes[inputIndex] = inputTx;
                }

                txIndex++;
                loadedTxes.Post(new LoadedTx(tx, txIndex, inputTxes.ToImmutableArray()));
            }
            loadedTxes.Complete();

            // validate block
            ValidationException ex;
            AssertMethods.AssertAggregateThrows<ValidationException>(() =>
                BlockValidator.ValidateBlock(coreStorage, rules, invalidChainedHeader, loadedTxes).Wait(), out ex);
            Assert.IsTrue(ex.Message.Contains("Merkle root is invalid"));
        }
    }
}
