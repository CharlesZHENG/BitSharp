using BitSharp.Common;
using BitSharp.Common.Test;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using System;
using System.Linq;
using System.Threading.Tasks.Dataflow;

namespace BitSharp.Core.Test.Builders
{
    [TestClass]
    public class ChainStateBuilderTest
    {
        [TestMethod]
        public void TestInitWithChain()
        {
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.GenesisChained();
            var header1 = fakeHeaders.NextChained();

            var rules = Mock.Of<ICoreRules>();
            var coreStorage = new Mock<ICoreStorage>();
            var storageManager = new Mock<IStorageManager>();
            var chainStateCursor = new Mock<IDeferredChainStateCursor>();

            storageManager.Setup(x => x.OpenChainStateCursor()).Returns(
                new DisposeHandle<IChainStateCursor>(_ => { }, chainStateCursor.Object));

            storageManager.Setup(x => x.OpenDeferredChainStateCursor(It.IsAny<IChainState>())).Returns(
                new DisposeHandle<IDeferredChainStateCursor>(_ => { }, chainStateCursor.Object));

            chainStateCursor.Setup(x => x.ChainTip).Returns(header1);
            chainStateCursor.Setup(x => x.TryGetHeader(header0.Hash, out header0)).Returns(true);
            chainStateCursor.Setup(x => x.TryGetHeader(header1.Hash, out header1)).Returns(true);

            var chainStateBuilder = new ChainStateBuilder(rules, coreStorage.Object, storageManager.Object);

            CollectionAssert.AreEqual(new[] { header0, header1 }, chainStateBuilder.Chain.Blocks);
        }

        [TestMethod]
        public void TestChainTipOutOfSync()
        {
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.GenesisChained();
            var header1 = fakeHeaders.NextChained();
            var header2 = fakeHeaders.NextChained();

            var rules = Mock.Of<ICoreRules>();
            var coreStorage = new Mock<ICoreStorage>();
            var storageManager = new Mock<IStorageManager>();
            var chainStateCursor = new Mock<IDeferredChainStateCursor>();

            storageManager.Setup(x => x.OpenChainStateCursor()).Returns(
                new DisposeHandle<IChainStateCursor>(_ => { }, chainStateCursor.Object));

            storageManager.Setup(x => x.OpenDeferredChainStateCursor(It.IsAny<IChainState>())).Returns(
                new DisposeHandle<IDeferredChainStateCursor>(_ => { }, chainStateCursor.Object));

            chainStateCursor.Setup(x => x.TryGetHeader(header0.Hash, out header0)).Returns(true);
            chainStateCursor.Setup(x => x.TryGetHeader(header1.Hash, out header1)).Returns(true);
            chainStateCursor.Setup(x => x.TryGetHeader(header2.Hash, out header2)).Returns(true);

            // return header 1 as the chain tip
            chainStateCursor.Setup(x => x.ChainTip).Returns(header1);

            // init chain state builder seeing header 1
            var chainStateBuilder = new ChainStateBuilder(rules, coreStorage.Object, storageManager.Object);
            Assert.AreEqual(header1.Hash, chainStateBuilder.Chain.LastBlock.Hash);

            // alter the chain tip outside of the chain state builder
            chainStateCursor.Setup(x => x.ChainTip).Returns(header2);

            // attempt to add block when out of sync
            ChainStateOutOfSyncException actualEx;
            AssertMethods.AssertAggregateThrows<ChainStateOutOfSyncException>(() =>
                chainStateBuilder.AddBlockAsync(header2, Enumerable.Empty<BlockTx>()).Wait(),
                out actualEx);

            Assert.AreEqual(header1.Hash, actualEx.ExpectedChainTip.Hash);
            Assert.AreEqual(header2.Hash, actualEx.ActualChainTip.Hash);

            // attempt to rollback block when out of sync
            AssertMethods.AssertThrows<ChainStateOutOfSyncException>(() =>
                chainStateBuilder.RollbackBlock(header2, Enumerable.Empty<BlockTx>()),
                out actualEx);

            Assert.AreEqual(header1.Hash, actualEx.ExpectedChainTip.Hash);
            Assert.AreEqual(header2.Hash, actualEx.ActualChainTip.Hash);
        }

        [TestMethod]
        public void TestMissingHeader()
        {
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.GenesisChained();
            var header1 = fakeHeaders.NextChained();
            var header2 = fakeHeaders.NextChained();

            var rules = Mock.Of<ICoreRules>();
            var coreStorage = new Mock<ICoreStorage>();
            var storageManager = new Mock<IStorageManager>();
            var chainStateCursor = new Mock<IChainStateCursor>();

            storageManager.Setup(x => x.OpenChainStateCursor()).Returns(
                new DisposeHandle<IChainStateCursor>(_ => { }, chainStateCursor.Object));

            // don't mock header 1 so it is missing
            chainStateCursor.Setup(x => x.TryGetHeader(header0.Hash, out header0)).Returns(true);
            chainStateCursor.Setup(x => x.TryGetHeader(header2.Hash, out header2)).Returns(true);

            // return header 2 as the chain tip
            chainStateCursor.Setup(x => x.ChainTip).Returns(header2);

            // init chain state builder with missing header
            StorageCorruptException actualEx;
            AssertMethods.AssertThrows<StorageCorruptException>(() =>
                {
                    var chain = new ChainStateBuilder(rules, coreStorage.Object, storageManager.Object).Chain;
                },
                out actualEx);

            Assert.AreEqual(StorageType.ChainState, actualEx.StorageType);
            Assert.AreEqual("ChainState is missing header.", actualEx.Message);
        }

        [TestMethod]
        public void TestInvalidMerkleRoot()
        {
            // prepare mocks
            var coreStorage = new Mock<ICoreStorage>();
            var storageManager = new Mock<IStorageManager>();
            var chainStateCursor = new Mock<IDeferredChainStateCursor>();

            storageManager.Setup(x => x.OpenChainStateCursor()).Returns(
                new DisposeHandle<IChainStateCursor>(_ => { }, chainStateCursor.Object));

            storageManager.Setup(x => x.OpenDeferredChainStateCursor(It.IsAny<IChainState>())).Returns(
                new DisposeHandle<IDeferredChainStateCursor>(_ => { }, chainStateCursor.Object));

            chainStateCursor.Setup(x => x.CursorCount).Returns(1);
            chainStateCursor.Setup(x => x.UtxoWorkQueue).Returns(Mock.Of<IDataflowBlock>());
            chainStateCursor.Setup(x => x.UtxoApplierBlock).Returns(Mock.Of<IDataflowBlock>());

            // prepare a test block
            var testBlocks = new TestBlocks();
            var rules = testBlocks.Rules;

            var block = testBlocks.MineAndAddBlock(txCount: 10);
            var chainedHeader = testBlocks.Chain.LastBlock;

            // create an invalid version of the header where the merkle root is incorrect
            var invalidChainedHeader = ChainedHeader.CreateFromPrev(rules.ChainParams.GenesisChainedHeader, block.Header.With(MerkleRoot: UInt256.Zero), DateTime.Now);

            // mock genesis block & chain tip
            var genesisHeader = rules.ChainParams.GenesisChainedHeader;
            chainStateCursor.Setup(x => x.ChainTip).Returns(genesisHeader);
            chainStateCursor.Setup(x => x.TryGetHeader(genesisHeader.Hash, out genesisHeader)).Returns(true);

            // mock invalid block
            chainStateCursor.Setup(x => x.TryGetHeader(chainedHeader.Hash, out invalidChainedHeader)).Returns(true);

            // init chain state builder
            var chainStateBuilder = new ChainStateBuilder(rules, coreStorage.Object, storageManager.Object);
            Assert.AreEqual(rules.ChainParams.GenesisBlock.Hash, chainStateBuilder.Chain.LastBlock.Hash);

            // attempt to add block with invalid merkle root
            ValidationException actualEx;
            AssertMethods.AssertAggregateThrows<ValidationException>(() =>
                chainStateBuilder.AddBlockAsync(invalidChainedHeader, Enumerable.Empty<BlockTx>()).Wait(),
                out actualEx);

            // verify error
            Assert.AreEqual($"Failing block {invalidChainedHeader.Hash} at height 1: Merkle root is invalid", actualEx.Message);
        }
    }
}
