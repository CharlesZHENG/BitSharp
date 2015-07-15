using BitSharp.Common;
using BitSharp.Common.Test;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

            var rules = Mock.Of<IBlockchainRules>();
            var coreStorage = new Mock<ICoreStorage>();
            var storageManager = new Mock<IStorageManager>();
            var chainStateCursor = new Mock<IChainStateCursor>();

            storageManager.Setup(x => x.OpenChainStateCursor()).Returns(
                new DisposeHandle<IChainStateCursor>(() => { }, chainStateCursor.Object));

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

            var rules = Mock.Of<IBlockchainRules>();
            var coreStorage = new Mock<ICoreStorage>();
            var storageManager = new Mock<IStorageManager>();
            var chainStateCursor = new Mock<IChainStateCursor>();

            storageManager.Setup(x => x.OpenChainStateCursor()).Returns(
                new DisposeHandle<IChainStateCursor>(() => { }, chainStateCursor.Object));

            chainStateCursor.Setup(x => x.TryGetHeader(header0.Hash, out header0)).Returns(true);
            chainStateCursor.Setup(x => x.TryGetHeader(header1.Hash, out header1)).Returns(true);
            chainStateCursor.Setup(x => x.TryGetHeader(header2.Hash, out header2)).Returns(true);

            // return header 1 as the chain tip
            chainStateCursor.Setup(x => x.ChainTip).Returns(header1);

            // init chain state builder seeing header 1
            var chainStateBuilder = new ChainStateBuilder(rules, coreStorage.Object, storageManager.Object);

            // alter the chain tip outside of the chain state builder
            chainStateCursor.Setup(x => x.ChainTip).Returns(header2);

            // attempt to add block when out of sync
            ChainStateOutOfSyncException actualEx;
            AssertMethods.AssertThrows<ChainStateOutOfSyncException>(() =>
                chainStateBuilder.AddBlock(header2, Enumerable.Empty<BlockTx>()),
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
    }
}
