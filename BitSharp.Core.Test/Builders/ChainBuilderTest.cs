using BitSharp.Common;
using BitSharp.Common.Test;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;

namespace BitSharp.Core.Test.Builders
{
    [TestClass]
    public class ChainBuilderTest
    {
        [TestMethod]
        public void TestCreateFromParentChain()
        {
            // create a chain
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.GenesisChained();
            var header1 = fakeHeaders.NextChained();
            var parentChain = new ChainBuilder(new[] { header0, header1 }).ToImmutable();

            // create builder from chain
            var chainBuilder = new ChainBuilder(parentChain);

            // verify
            CollectionAssert.AreEqual(new[] { header0, header1 }, chainBuilder.Blocks);
            CollectionAssert.AreEquivalent(new Dictionary<UInt256, ChainedHeader> { { header0.Hash, header0 }, { header1.Hash, header1 } }, chainBuilder.BlocksByHash);
        }

        [TestMethod]
        public void TestCreateFromEnumerable()
        {
            // create a chain
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.GenesisChained();
            var header1 = fakeHeaders.NextChained();

            // create builder from enumerable
            var chainBuilder = new ChainBuilder(new[] { header0, header1 });

            // verify
            CollectionAssert.AreEqual(new[] { header0, header1 }, chainBuilder.Blocks);
            CollectionAssert.AreEquivalent(new Dictionary<UInt256, ChainedHeader> { { header0.Hash, header0 }, { header1.Hash, header1 } }, chainBuilder.BlocksByHash);
        }

        [TestMethod]
        public void TestCreateFromInvalidEnumerable()
        {
            // create a chain
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.GenesisChained();
            var header1 = fakeHeaders.NextChained();

            // create builder from out of order enumerable
            AssertMethods.AssertThrows<InvalidOperationException>(() => new ChainBuilder(new[] { header1, header0 }));
        }

        [TestMethod]
        public void TestGenesisBlock()
        {
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.GenesisChained();
            var header1 = fakeHeaders.NextChained();

            var chainBuilder = new ChainBuilder();

            // verify genesis with 0 blocks
            Assert.IsNull(chainBuilder.GenesisBlock);

            // verify genesis with 1 block
            chainBuilder.AddBlock(header0);
            Assert.AreEqual(header0, chainBuilder.GenesisBlock);

            // verify genesis with 2 blocks
            chainBuilder.AddBlock(header1);
            Assert.AreEqual(header0, chainBuilder.GenesisBlock);
        }

        [TestMethod]
        public void TestLastBlock()
        {
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.GenesisChained();
            var header1 = fakeHeaders.NextChained();

            var chainBuilder = new ChainBuilder();

            // verify last block with 0 blocks
            Assert.IsNull(chainBuilder.LastBlock);

            // verify last block with 1 block
            chainBuilder.AddBlock(header0);
            Assert.AreEqual(header0, chainBuilder.LastBlock);

            // verify last block with 2 blocks
            chainBuilder.AddBlock(header1);
            Assert.AreEqual(header1, chainBuilder.LastBlock);
        }

        [TestMethod]
        public void TestHeight()
        {
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.GenesisChained();
            var header1 = fakeHeaders.NextChained();

            var chainBuilder = new ChainBuilder();

            // verify height with 0 blocks
            Assert.AreEqual(-1, chainBuilder.Height);

            // verify height with 1 block
            chainBuilder.AddBlock(header0);
            Assert.AreEqual(0, chainBuilder.Height);

            // verify height with 2 blocks
            chainBuilder.AddBlock(header1);
            Assert.AreEqual(1, chainBuilder.Height);
        }

        [TestMethod]
        public void TestTotalWork()
        {
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.GenesisChained();
            var header1 = fakeHeaders.NextChained();
            var totalWork0 = DataCalculator.CalculateWork(header0);
            var totalWork1 = totalWork0 + DataCalculator.CalculateWork(header1);

            var chainBuilder = new ChainBuilder();

            // verify total work with 0 blocks
            Assert.AreEqual(0, chainBuilder.TotalWork);

            // verify total work with 1 block
            chainBuilder.AddBlock(header0);
            Assert.AreEqual(totalWork0, chainBuilder.TotalWork);

            // verify total work with 2 blocks
            chainBuilder.AddBlock(header1);
            Assert.AreEqual(totalWork1, chainBuilder.TotalWork);
        }

        [TestMethod]
        public void TestBlocks()
        {
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.GenesisChained();
            var header1 = fakeHeaders.NextChained();

            var chainBuilder = new ChainBuilder();

            // verify block list with 0 blocks
            Assert.AreEqual(0, chainBuilder.Blocks.Count);

            // verify block list with 1 block
            chainBuilder.AddBlock(header0);
            CollectionAssert.AreEqual(new[] { header0 }, chainBuilder.Blocks);

            // verify block list with 2 blocks
            chainBuilder.AddBlock(header1);
            CollectionAssert.AreEqual(new[] { header0, header1 }, chainBuilder.Blocks);
        }

        [TestMethod]
        public void TestBlocksByHash()
        {
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.GenesisChained();
            var header1 = fakeHeaders.NextChained();

            var chainBuilder = new ChainBuilder();

            // verify blocks dictionary with 0 blocks
            Assert.AreEqual(0, chainBuilder.BlocksByHash.Count);

            // verify blocks dictionary with 1 block
            chainBuilder.AddBlock(header0);
            CollectionAssert.AreEquivalent(new Dictionary<UInt256, ChainedHeader> { { header0.Hash, header0 } }, chainBuilder.BlocksByHash);

            // verify blocks dictionary with 2 blocks
            chainBuilder.AddBlock(header1);
            CollectionAssert.AreEquivalent(new Dictionary<UInt256, ChainedHeader> { { header0.Hash, header0 }, { header1.Hash, header1 } }, chainBuilder.BlocksByHash);
        }

        [TestMethod]
        public void TestAddBlock()
        {
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.GenesisChained();
            var header1 = fakeHeaders.NextChained();

            var chainBuilder = new ChainBuilder();

            // add header 0 and verify
            chainBuilder.AddBlock(header0);
            CollectionAssert.AreEqual(new[] { header0 }, chainBuilder.Blocks);
            CollectionAssert.AreEquivalent(new Dictionary<UInt256, ChainedHeader> { { header0.Hash, header0 } }, chainBuilder.BlocksByHash);

            // add header 1 and verify
            chainBuilder.AddBlock(header1);
            CollectionAssert.AreEqual(new[] { header0, header1 }, chainBuilder.Blocks);
            CollectionAssert.AreEquivalent(new Dictionary<UInt256, ChainedHeader> { { header0.Hash, header0 }, { header1.Hash, header1 } }, chainBuilder.BlocksByHash);
        }

        [TestMethod]
        public void TestAddBlockInvalid()
        {
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.GenesisChained();
            var header1 = fakeHeaders.NextChained();
            var header2 = fakeHeaders.NextChained();

            var chainBuilder = new ChainBuilder();

            // adding header 1 first should fail
            AssertMethods.AssertThrows<InvalidOperationException>(() => chainBuilder.AddBlock(header1));

            // add header 0
            chainBuilder.AddBlock(header0);

            // adding header 2 without header 1 should fail
            AssertMethods.AssertThrows<InvalidOperationException>(() => chainBuilder.AddBlock(header2));
        }

        [TestMethod]
        public void TestRemoveBlock()
        {
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.GenesisChained();
            var header1 = fakeHeaders.NextChained();

            var chainBuilder = new ChainBuilder(new[] { header0, header1 });

            // remove header 1 and verify
            chainBuilder.RemoveBlock(header1);
            CollectionAssert.AreEqual(new[] { header0 }, chainBuilder.Blocks);
            CollectionAssert.AreEquivalent(new Dictionary<UInt256, ChainedHeader> { { header0.Hash, header0 } }, chainBuilder.BlocksByHash);

            // remove header 0 and verify
            chainBuilder.RemoveBlock(header0);
            Assert.AreEqual(0, chainBuilder.Blocks.Count);
            Assert.AreEqual(0, chainBuilder.BlocksByHash.Count);
        }

        [TestMethod]
        public void TestRemoveBlockInvalid()
        {
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.GenesisChained();
            var header1 = fakeHeaders.NextChained();
            var header2 = fakeHeaders.NextChained();

            var chainBuilder = new ChainBuilder(new[] { header0, header1, header2 });

            // removing header 1 first should fail
            AssertMethods.AssertThrows<InvalidOperationException>(() => chainBuilder.RemoveBlock(header1));

            // remove header 2
            chainBuilder.RemoveBlock(header2);

            // removing header 0 with header 1 present should fail
            AssertMethods.AssertThrows<InvalidOperationException>(() => chainBuilder.AddBlock(header0));
        }

        [TestMethod]
        public void TestToImmutable()
        {
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.GenesisChained();
            var header1 = fakeHeaders.NextChained();

            var chainBuilder = new ChainBuilder();

            // verify to builder with 0 blocks
            var chainEmpty = chainBuilder.ToImmutable();
            Assert.AreEqual(0, chainEmpty.Blocks.Count);
            Assert.AreEqual(0, chainEmpty.BlocksByHash.Count);

            // verify to builder with 1 block
            chainBuilder.AddBlock(header0);
            var chain0 = chainBuilder.ToImmutable();
            CollectionAssert.AreEqual(new[] { header0 }, chain0.Blocks);
            CollectionAssert.AreEquivalent(new Dictionary<UInt256, ChainedHeader> { { header0.Hash, header0 } }, chain0.BlocksByHash);

            // verify to builder with 2 blocks
            chainBuilder.AddBlock(header1);
            var chain1 = chainBuilder.ToImmutable();
            CollectionAssert.AreEqual(new[] { header0, header1 }, chain1.Blocks);
            CollectionAssert.AreEquivalent(new Dictionary<UInt256, ChainedHeader> { { header0.Hash, header0 }, { header1.Hash, header1 } }, chain1.BlocksByHash);
        }
    }
}
