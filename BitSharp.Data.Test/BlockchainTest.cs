﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BitSharp.Data.Test
{
    [TestClass]
    public class BlockchainTest
    {
        [TestMethod]
        public void TestBlockchainEquality()
        {
            var randomBlockchain = RandomData.RandomBlockchain();

            var sameBlockchain = new Blockchain
            (
                blockList: ImmutableList.Create(randomBlockchain.BlockList.ToArray()),
                blockListHashes: ImmutableHashSet.Create(randomBlockchain.BlockListHashes.ToArray()),
                utxo: randomBlockchain.Utxo.ToDictionary(x => x.Key, x => x.Value).ToImmutableDictionary(x => x.Key, x => x.Value)
            );

            var newChainedBlock = randomBlockchain.BlockList.Last();
            newChainedBlock = new ChainedBlock(newChainedBlock.BlockHash, newChainedBlock.PreviousBlockHash, newChainedBlock.Height + 1, newChainedBlock.TotalWork);
            var differentBlockchainBlockList = new Blockchain
            (
                blockList: randomBlockchain.BlockList.Add(newChainedBlock),
                blockListHashes: randomBlockchain.BlockListHashes,
                utxo: randomBlockchain.Utxo
            );

            var differentBlockchainBlockListHashes = new Blockchain
            (
                blockList: randomBlockchain.BlockList,
                blockListHashes: randomBlockchain.BlockListHashes.Remove(randomBlockchain.BlockListHashes.Last()),
                utxo: randomBlockchain.Utxo
            );

            var differentBlockchainUtxo = new Blockchain
            (
                blockList: randomBlockchain.BlockList,
                blockListHashes: randomBlockchain.BlockListHashes,
                utxo: randomBlockchain.Utxo.Remove(randomBlockchain.Utxo.Keys.Last())
            );

            Assert.IsTrue(randomBlockchain.Equals(sameBlockchain));
            Assert.IsTrue(randomBlockchain == sameBlockchain);
            Assert.IsFalse(randomBlockchain != sameBlockchain);

            Assert.IsFalse(randomBlockchain.Equals(differentBlockchainBlockList));
            Assert.IsFalse(randomBlockchain == differentBlockchainBlockList);
            Assert.IsTrue(randomBlockchain != differentBlockchainBlockList);

            Assert.IsFalse(randomBlockchain.Equals(differentBlockchainBlockListHashes));
            Assert.IsFalse(randomBlockchain == differentBlockchainBlockListHashes);
            Assert.IsTrue(randomBlockchain != differentBlockchainBlockListHashes);

            Assert.IsFalse(randomBlockchain.Equals(differentBlockchainUtxo));
            Assert.IsFalse(randomBlockchain == differentBlockchainUtxo);
            Assert.IsTrue(randomBlockchain != differentBlockchainUtxo);
        }

        [TestMethod]
        public void TestBlockchainBlockCount()
        {
            var randomBlockchain = RandomData.RandomBlockchain();
            Assert.AreEqual(randomBlockchain.BlockList.Count, randomBlockchain.BlockCount);
        }

        [TestMethod]
        public void TestBlockchainHeight()
        {
            var randomBlockchain = RandomData.RandomBlockchain();
            Assert.AreEqual(randomBlockchain.BlockList.Count - 1, randomBlockchain.Height);
        }

        [TestMethod]
        public void TestBlockchainTotalWork()
        {
            var randomBlockchain = RandomData.RandomBlockchain();
            Assert.AreEqual(randomBlockchain.BlockList.Last().TotalWork, randomBlockchain.TotalWork);
        }

        [TestMethod]
        public void TestBlockchainRootBlock()
        {
            var randomBlockchain = RandomData.RandomBlockchain();
            Assert.AreEqual(randomBlockchain.BlockList.Last(), randomBlockchain.RootBlock);
        }

        [TestMethod]
        public void TestBlockchainRootBlockHash()
        {
            var randomBlockchain = RandomData.RandomBlockchain();
            Assert.AreEqual(randomBlockchain.BlockList.Last().BlockHash, randomBlockchain.RootBlockHash);
        }
    }
}
