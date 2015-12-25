using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Common.Test;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
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
            var validatableTxes = new BufferBlock<ValidatableTx>();
            var txIndex = -1;
            foreach (var tx in block.Transactions)
            {
                txIndex++;

                var prevTxOutputs = new TxOutput[txIndex > 0 ? tx.Inputs.Length : 0];
                for (var inputIndex = 0; inputIndex < prevTxOutputs.Length; inputIndex++)
                {
                    prevTxOutputs[inputIndex] = RandomData.RandomTxOutput();
                }

                var blockTx = BlockTx.Create(txIndex, tx);
                validatableTxes.Post(new ValidatableTx(blockTx, invalidChainedHeader, prevTxOutputs.ToImmutableArray()));
            }
            validatableTxes.Complete();

            // validate block
            ValidationException ex;
            AssertMethods.AssertAggregateThrows<ValidationException>(() =>
                BlockValidator.ValidateBlockAsync(coreStorage, rules, invalidChainedHeader, validatableTxes).Wait(), out ex);
            Assert.IsTrue(ex.Message.Contains("Merkle root is invalid"));
        }
    }
}
