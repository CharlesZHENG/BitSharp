using BitSharp.Common;
using BitSharp.Common.Test;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;

namespace BitSharp.Core.Test.Domain
{
    [TestClass]
    public class ChainTest
    {
        [TestMethod]
        public void TestGenesisBlock()
        {
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.GenesisChained();
            var header1 = fakeHeaders.NextChained();

            var chainBuilder = new ChainBuilder();

            // verify genesis with 0 blocks
            var chainEmpty = chainBuilder.ToImmutable();
            Assert.IsNull(chainEmpty.GenesisBlock);

            // verify genesis with 1 block
            chainBuilder.AddBlock(header0);
            var chain0 = chainBuilder.ToImmutable();
            Assert.AreEqual(header0, chain0.GenesisBlock);

            // verify genesis with 2 blocks
            chainBuilder.AddBlock(header1);
            var chain1 = chainBuilder.ToImmutable();
            Assert.AreEqual(header0, chain1.GenesisBlock);
        }

        [TestMethod]
        public void TestLastBlock()
        {
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.GenesisChained();
            var header1 = fakeHeaders.NextChained();

            var chainBuilder = new ChainBuilder();

            // verify last block with 0 blocks
            var chainEmpty = chainBuilder.ToImmutable();
            Assert.IsNull(chainEmpty.LastBlock);

            // verify last block with 1 block
            chainBuilder.AddBlock(header0);
            var chain0 = chainBuilder.ToImmutable();
            Assert.AreEqual(header0, chain0.LastBlock);

            // verify last block with 2 blocks
            chainBuilder.AddBlock(header1);
            var chain1 = chainBuilder.ToImmutable();
            Assert.AreEqual(header1, chain1.LastBlock);
        }

        [TestMethod]
        public void TestHeight()
        {
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.GenesisChained();
            var header1 = fakeHeaders.NextChained();

            var chainBuilder = new ChainBuilder();

            // verify height with 0 blocks
            var chainEmpty = chainBuilder.ToImmutable();
            Assert.AreEqual(-1, chainEmpty.Height);

            // verify height with 1 block
            chainBuilder.AddBlock(header0);
            var chain0 = chainBuilder.ToImmutable();
            Assert.AreEqual(0, chain0.Height);

            // verify height with 2 blocks
            chainBuilder.AddBlock(header1);
            var chain1 = chainBuilder.ToImmutable();
            Assert.AreEqual(1, chain1.Height);
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
            var chainEmpty = chainBuilder.ToImmutable();
            Assert.AreEqual(0, chainEmpty.TotalWork);

            // verify total work with 1 block
            chainBuilder.AddBlock(header0);
            var chain0 = chainBuilder.ToImmutable();
            Assert.AreEqual(totalWork0.ToBigInteger(), chain0.TotalWork);

            // verify total work with 2 blocks
            chainBuilder.AddBlock(header1);
            var chain1 = chainBuilder.ToImmutable();
            Assert.AreEqual(totalWork1.ToBigInteger(), chain1.TotalWork);
        }

        [TestMethod]
        public void TestBlocks()
        {
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.GenesisChained();
            var header1 = fakeHeaders.NextChained();

            var chainBuilder = new ChainBuilder();

            // verify block list with 0 blocks
            var chainEmpty = chainBuilder.ToImmutable();
            Assert.AreEqual(0, chainEmpty.Blocks.Count);

            // verify block list with 1 block
            chainBuilder.AddBlock(header0);
            var chain0 = chainBuilder.ToImmutable();
            CollectionAssert.AreEqual(new[] { header0 }, chain0.Blocks);

            // verify block list with 2 blocks
            chainBuilder.AddBlock(header1);
            var chain1 = chainBuilder.ToImmutable();
            CollectionAssert.AreEqual(new[] { header0, header1 }, chain1.Blocks);
        }

        [TestMethod]
        public void TestBlocksByHash()
        {
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.GenesisChained();
            var header1 = fakeHeaders.NextChained();

            var chainBuilder = new ChainBuilder();

            // verify blocks dictionary with 0 blocks
            var chainEmpty = chainBuilder.ToImmutable();
            Assert.AreEqual(0, chainEmpty.BlocksByHash.Count);

            // verify blocks dictionary with 1 block
            chainBuilder.AddBlock(header0);
            var chain0 = chainBuilder.ToImmutable();
            CollectionAssert.AreEquivalent(new Dictionary<UInt256, ChainedHeader> { { header0.Hash, header0 } }, chain0.BlocksByHash);

            // verify blocks dictionary with 2 blocks
            chainBuilder.AddBlock(header1);
            var chain1 = chainBuilder.ToImmutable();
            CollectionAssert.AreEquivalent(new Dictionary<UInt256, ChainedHeader> { { header0.Hash, header0 }, { header1.Hash, header1 } }, chain1.BlocksByHash);
        }

        [TestMethod]
        public void TestNavigateTowards()
        {
            // create forked chains
            var fakeHeadersA = new FakeHeaders();
            var header0 = fakeHeadersA.GenesisChained();
            var header1 = fakeHeadersA.NextChained();

            var fakeHeadersB = new FakeHeaders(fakeHeadersA);
            var header2A = fakeHeadersA.NextChained();
            var header3A = fakeHeadersA.NextChained();
            var header4A = fakeHeadersA.NextChained();
            var header2B = fakeHeadersB.NextChained();
            var header3B = fakeHeadersB.NextChained();
            var header4B = fakeHeadersB.NextChained();

            var chain0 = new ChainBuilder(new[] { header0, header1, header2A, header3A }).ToImmutable();
            var chain1 = new ChainBuilder(new[] { header0, header1, header2B, header3B, header4B }).ToImmutable();

            // verify path from chain 0 to chain 1
            CollectionAssert.AreEqual(
                new[]
                {
                    Tuple.Create(-1, header3A),
                    Tuple.Create(-1, header2A),
                    Tuple.Create(+1, header2B),
                    Tuple.Create(+1, header3B),
                    Tuple.Create(+1, header4B)
                }
                , chain0.NavigateTowards(chain1).ToList());

            // verify path from chain 1 to chain 0
            CollectionAssert.AreEqual(
                new[]
                {
                    Tuple.Create(-1, header4B),
                    Tuple.Create(-1, header3B),
                    Tuple.Create(-1, header2B),
                    Tuple.Create(+1, header2A),
                    Tuple.Create(+1, header3A)
                }
                , chain1.NavigateTowards(chain0).ToList());
        }

        [TestMethod]
        public void TestNavigateTowardsFunc()
        {
            // create chains
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.GenesisChained();
            var header1 = fakeHeaders.NextChained();
            var header2 = fakeHeaders.NextChained();
            var header3 = fakeHeaders.NextChained();

            var chain0 = new ChainBuilder(new[] { header0, }).ToImmutable();
            var chain1 = new ChainBuilder(new[] { header0, header1, }).ToImmutable();
            var chain2 = new ChainBuilder(new[] { header0, header1, header2 }).ToImmutable();
            var chain3 = new ChainBuilder(new[] { header0, header1, header2, header3 }).ToImmutable();

            // the list of target chains to use, stays 1 ahead and then catches up with chain 3
            var targetStack = new Stack<Chain>(new[] { chain1, chain2, chain3, chain3 }.Reverse());

            // verify navigating towards an updating chain
            CollectionAssert.AreEqual(
                new[]
                {
                    Tuple.Create(+1, header1),
                    Tuple.Create(+1, header2),
                    Tuple.Create(+1, header3)
                }
                , chain0.NavigateTowards(() => targetStack.Pop()).ToList());

            // verify all targets used
            Assert.AreEqual(0, targetStack.Count);
        }

        [TestMethod]
        public void TestNavigateTowardsEmpty()
        {
            // create chain
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.GenesisChained();
            var header1 = fakeHeaders.NextChained();
            var header2 = fakeHeaders.NextChained();

            var chainEmpty = new ChainBuilder().ToImmutable();
            var chain = new ChainBuilder(new[] { header0, header1, header2 }).ToImmutable();

            // verify chaining to empty does nothing
            Assert.AreEqual(0, chainEmpty.NavigateTowards(chainEmpty).Count());
            Assert.AreEqual(0, chain.NavigateTowards(chainEmpty).Count());

            // verify path from empty chain to chain
            CollectionAssert.AreEqual(
                new[]
                {
                    Tuple.Create(+1, header0),
                    Tuple.Create(+1, header1),
                    Tuple.Create(+1, header2)
                }
                , chainEmpty.NavigateTowards(chain).ToList());
        }

        [TestMethod]
        public void TestNavigateTowardsInvalidChains()
        {
            // create distinct chains
            var fakeHeadersA = new FakeHeaders();
            var header0A = fakeHeadersA.GenesisChained();
            var header1A = fakeHeadersA.NextChained();

            var fakeHeadersB = new FakeHeaders();
            var header0B = fakeHeadersB.GenesisChained();
            var header1B = fakeHeadersB.NextChained();

            var chainEmpty = new ChainBuilder().ToImmutable();
            var chainA = new ChainBuilder(new[] { header0A, header1A }).ToImmutable();
            var chainB = new ChainBuilder(new[] { header0B, header1B, }).ToImmutable();

            // unrelated chains should error
            AssertMethods.AssertThrows<InvalidOperationException>(() => chainA.NavigateTowards(chainB).ToList());
            AssertMethods.AssertThrows<InvalidOperationException>(() => chainB.NavigateTowards(chainA).ToList());
        }

        [TestMethod]
        public void TestToBuilder()
        {
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.GenesisChained();
            var header1 = fakeHeaders.NextChained();

            var chainBuilder = new ChainBuilder();

            // verify to builder with 0 blocks
            var chainEmpty = chainBuilder.ToImmutable();
            var chainBuilderEmpty = chainEmpty.ToBuilder();
            Assert.AreEqual(0, chainBuilderEmpty.Blocks.Count);
            Assert.AreEqual(0, chainBuilderEmpty.BlocksByHash.Count);

            // verify to builder with 1 block
            chainBuilder.AddBlock(header0);
            var chain0 = chainBuilder.ToImmutable();
            var chainBuilder0 = chain0.ToBuilder();
            CollectionAssert.AreEqual(new[] { header0 }, chainBuilder0.Blocks);
            CollectionAssert.AreEquivalent(new Dictionary<UInt256, ChainedHeader> { { header0.Hash, header0 } }, chainBuilder0.BlocksByHash);

            // verify to builder with 2 blocks
            chainBuilder.AddBlock(header1);
            var chain1 = chainBuilder.ToImmutable();
            var chainBuilder1 = chain1.ToBuilder();
            CollectionAssert.AreEqual(new[] { header0, header1 }, chainBuilder1.Blocks);
            CollectionAssert.AreEquivalent(new Dictionary<UInt256, ChainedHeader> { { header0.Hash, header0 }, { header1.Hash, header1 } }, chainBuilder1.BlocksByHash);
        }

        [TestMethod]
        public void TestCreateForGenesisBlock()
        {
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.GenesisChained();

            var chain = Chain.CreateForGenesisBlock(header0);

            CollectionAssert.AreEqual(new[] { header0 }, chain.Blocks);
        }

        [TestMethod]
        public void TestReadChain()
        {
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.GenesisChained();
            var header1 = fakeHeaders.NextChained();
            var header2 = fakeHeaders.NextChained();

            var chainedHeaders = new Dictionary<UInt256, ChainedHeader>();
            chainedHeaders.Add(header0.Hash, header0);
            chainedHeaders.Add(header1.Hash, header1);
            chainedHeaders.Add(header2.Hash, header2);

            Chain chain;
            Assert.IsTrue(Chain.TryReadChain(header2.Hash, out chain,
                headerHash =>
                {
                    ChainedHeader chainedHeader;
                    chainedHeaders.TryGetValue(headerHash, out chainedHeader);
                    return chainedHeader;
                }));

            CollectionAssert.AreEqual(fakeHeaders.ChainedHeaders, chain.Blocks);
        }

        [TestMethod]
        public void TestReadChainWithGap()
        {
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.GenesisChained();
            var header1 = fakeHeaders.NextChained();
            var header2 = fakeHeaders.NextChained();

            var chainedHeaders = new Dictionary<UInt256, ChainedHeader>();
            chainedHeaders.Add(header0.Hash, header0);
            chainedHeaders.Add(header2.Hash, header2);

            Chain chain;
            Assert.IsFalse(Chain.TryReadChain(header2.Hash, out chain,
                headerHash =>
                {
                    ChainedHeader chainedHeader;
                    chainedHeaders.TryGetValue(headerHash, out chainedHeader);
                    return chainedHeader;
                }));
        }
    }
}
