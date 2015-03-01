using BitSharp.Common;
using BitSharp.Core;
using BitSharp.Core.Domain;
using BitSharp.Core.Test;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Blockchain.Test
{
    [TestClass]
    public class BlockchainWalkerTest
    {
        private ChainedHeader chainedHeader0;
        private ChainedHeader chainedHeader1;

        private ChainedHeader chainedHeaderA2;
        private ChainedHeader chainedHeaderA3;
        private ChainedHeader chainedHeaderA4;

        private ChainedHeader chainedHeaderB2;
        private ChainedHeader chainedHeaderB3;
        private ChainedHeader chainedHeaderB4;

        private ChainedHeader chainedHeaderX0;
        private ChainedHeader chainedHeaderX1;

        private ImmutableDictionary<UInt256, ChainedHeader> chain;
        private Func<UInt256, ChainedHeader> getChainedHeader;

        [TestInitialize]
        public void Init()
        {
            var fakeHeaders = new FakeHeaders();
            this.chainedHeader0 = fakeHeaders.GenesisChained();
            this.chainedHeader1 = fakeHeaders.NextChained();

            var fakeHeadersA = new FakeHeaders(fakeHeaders);
            this.chainedHeaderA2 = fakeHeadersA.NextChained();
            this.chainedHeaderA3 = fakeHeadersA.NextChained();
            this.chainedHeaderA4 = fakeHeadersA.NextChained();

            var fakeHeadersB = new FakeHeaders(fakeHeaders);
            this.chainedHeaderB2 = fakeHeadersB.NextChained();
            this.chainedHeaderB3 = fakeHeadersB.NextChained();
            this.chainedHeaderB4 = fakeHeadersB.NextChained();

            var fakeHeadersX = new FakeHeaders();
            this.chainedHeaderX0 = fakeHeadersX.GenesisChained();
            this.chainedHeaderX1 = fakeHeadersX.NextChained();

            this.chain = ImmutableDictionary.CreateRange(
                new[] { chainedHeader0, chainedHeader1, chainedHeaderA2, chainedHeaderA3, chainedHeaderA4, chainedHeaderB2, chainedHeaderB3, chainedHeaderB4, chainedHeaderX0, chainedHeaderX1 }
                .Select(x => new KeyValuePair<UInt256, ChainedHeader>(x.Hash, x)));

            this.getChainedHeader = blockHash => this.chain[blockHash];
        }

        [TestMethod]
        public void TestSameBlock()
        {
            var walker = new BlockchainWalker();

            var path = walker.GetBlockchainPath(this.chainedHeaderA2, this.chainedHeaderA2, this.getChainedHeader);

            Assert.AreEqual(this.chainedHeaderA2, path.FromBlock);
            Assert.AreEqual(this.chainedHeaderA2, path.ToBlock);
            Assert.AreEqual(this.chainedHeaderA2, path.LastCommonBlock);

            Assert.AreEqual(0, path.RewindBlocks.Count);
            Assert.AreEqual(0, path.AdvanceBlocks.Count);
        }

        [TestMethod]
        public void TestAdvanceFromGenesis()
        {
            var walker = new BlockchainWalker();

            var path = walker.GetBlockchainPath(this.chainedHeader0, this.chainedHeaderA4, this.getChainedHeader);

            Assert.AreEqual(this.chainedHeader0, path.FromBlock);
            Assert.AreEqual(this.chainedHeaderA4, path.ToBlock);
            Assert.AreEqual(this.chainedHeader0, path.LastCommonBlock);

            Assert.AreEqual(0, path.RewindBlocks.Count);

            Assert.AreEqual(4, path.AdvanceBlocks.Count);
            Assert.AreEqual(this.chainedHeader1, path.AdvanceBlocks[0]);
            Assert.AreEqual(this.chainedHeaderA2, path.AdvanceBlocks[1]);
            Assert.AreEqual(this.chainedHeaderA3, path.AdvanceBlocks[2]);
            Assert.AreEqual(this.chainedHeaderA4, path.AdvanceBlocks[3]);
        }

        [TestMethod]
        public void TestAdvanceOnly()
        {
            var walker = new BlockchainWalker();

            var path = walker.GetBlockchainPath(this.chainedHeaderA2, this.chainedHeaderA4, this.getChainedHeader);

            Assert.AreEqual(this.chainedHeaderA2, path.FromBlock);
            Assert.AreEqual(this.chainedHeaderA4, path.ToBlock);
            Assert.AreEqual(this.chainedHeaderA2, path.LastCommonBlock);

            Assert.AreEqual(0, path.RewindBlocks.Count);

            Assert.AreEqual(2, path.AdvanceBlocks.Count);
            Assert.AreEqual(this.chainedHeaderA3, path.AdvanceBlocks[0]);
            Assert.AreEqual(this.chainedHeaderA4, path.AdvanceBlocks[1]);
        }

        [TestMethod]
        public void TestRewindOnly()
        {
            var walker = new BlockchainWalker();

            var path = walker.GetBlockchainPath(this.chainedHeaderA4, this.chainedHeaderA2, this.getChainedHeader);

            Assert.AreEqual(this.chainedHeaderA4, path.FromBlock);
            Assert.AreEqual(this.chainedHeaderA2, path.ToBlock);
            Assert.AreEqual(this.chainedHeaderA2, path.LastCommonBlock);

            Assert.AreEqual(2, path.RewindBlocks.Count);
            Assert.AreEqual(this.chainedHeaderA4, path.RewindBlocks[0]);
            Assert.AreEqual(this.chainedHeaderA3, path.RewindBlocks[1]);

            Assert.AreEqual(0, path.AdvanceBlocks.Count);
        }

        [TestMethod]
        public void TestRewindAndAdvanceFromHigher()
        {
            var walker = new BlockchainWalker();

            var path = walker.GetBlockchainPath(this.chainedHeaderA4, this.chainedHeaderB3, this.getChainedHeader);

            Assert.AreEqual(this.chainedHeaderA4, path.FromBlock);
            Assert.AreEqual(this.chainedHeaderB3, path.ToBlock);
            Assert.AreEqual(this.chainedHeader1, path.LastCommonBlock);

            Assert.AreEqual(3, path.RewindBlocks.Count);
            Assert.AreEqual(this.chainedHeaderA4, path.RewindBlocks[0]);
            Assert.AreEqual(this.chainedHeaderA3, path.RewindBlocks[1]);
            Assert.AreEqual(this.chainedHeaderA2, path.RewindBlocks[2]);

            Assert.AreEqual(2, path.AdvanceBlocks.Count);
            Assert.AreEqual(this.chainedHeaderB2, path.AdvanceBlocks[0]);
            Assert.AreEqual(this.chainedHeaderB3, path.AdvanceBlocks[1]);
        }

        [TestMethod]
        public void TestRewindAndAdvanceToHigher()
        {
            var walker = new BlockchainWalker();

            var path = walker.GetBlockchainPath(this.chainedHeaderA3, this.chainedHeaderB4, this.getChainedHeader);

            Assert.AreEqual(this.chainedHeaderA3, path.FromBlock);
            Assert.AreEqual(this.chainedHeaderB4, path.ToBlock);
            Assert.AreEqual(this.chainedHeader1, path.LastCommonBlock);

            Assert.AreEqual(2, path.RewindBlocks.Count);
            Assert.AreEqual(this.chainedHeaderA3, path.RewindBlocks[0]);
            Assert.AreEqual(this.chainedHeaderA2, path.RewindBlocks[1]);

            Assert.AreEqual(3, path.AdvanceBlocks.Count);
            Assert.AreEqual(this.chainedHeaderB2, path.AdvanceBlocks[0]);
            Assert.AreEqual(this.chainedHeaderB3, path.AdvanceBlocks[1]);
            Assert.AreEqual(this.chainedHeaderB4, path.AdvanceBlocks[2]);
        }

        [TestMethod]
        public void TestRewindAndAdvanceSameHeight()
        {
            var walker = new BlockchainWalker();

            var path = walker.GetBlockchainPath(this.chainedHeaderA4, this.chainedHeaderB4, this.getChainedHeader);

            Assert.AreEqual(this.chainedHeaderA4, path.FromBlock);
            Assert.AreEqual(this.chainedHeaderB4, path.ToBlock);
            Assert.AreEqual(this.chainedHeader1, path.LastCommonBlock);

            Assert.AreEqual(3, path.RewindBlocks.Count);
            Assert.AreEqual(this.chainedHeaderA4, path.RewindBlocks[0]);
            Assert.AreEqual(this.chainedHeaderA3, path.RewindBlocks[1]);
            Assert.AreEqual(this.chainedHeaderA2, path.RewindBlocks[2]);

            Assert.AreEqual(3, path.AdvanceBlocks.Count);
            Assert.AreEqual(this.chainedHeaderB2, path.AdvanceBlocks[0]);
            Assert.AreEqual(this.chainedHeaderB3, path.AdvanceBlocks[1]);
            Assert.AreEqual(this.chainedHeaderB4, path.AdvanceBlocks[2]);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void TestChainMismatch()
        {
            var walker = new BlockchainWalker();

            walker.GetBlockchainPath(this.chainedHeaderA4, this.chainedHeaderX0, this.getChainedHeader);
        }
    }
}
