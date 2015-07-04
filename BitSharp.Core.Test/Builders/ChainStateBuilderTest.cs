using BitSharp.Common;
using BitSharp.Core.Builders;
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
    }
}
