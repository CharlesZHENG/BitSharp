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
    public class BlockTest
    {
        [TestMethod]
        public void TestBlockEquality()
        {
            var randomBlock = RandomData.RandomBlock();

            var sameBlock = new Block
            (
                header: new BlockHeader(randomBlock.Header.Version, randomBlock.Header.PreviousBlock, randomBlock.Header.MerkleRoot, randomBlock.Header.Time, randomBlock.Header.Bits, randomBlock.Header.Nonce),
                transactions: ImmutableList.Create(randomBlock.Transactions.ToArray())
            );

            var differentBlock = randomBlock.With(Header: randomBlock.Header.With(Bits: ~randomBlock.Header.Bits));

            Assert.IsTrue(randomBlock.Equals(sameBlock));
            Assert.IsTrue(randomBlock == sameBlock);
            Assert.IsFalse(randomBlock != sameBlock);

            Assert.IsFalse(randomBlock.Equals(differentBlock));
            Assert.IsFalse(randomBlock == differentBlock);
            Assert.IsTrue(randomBlock != differentBlock);
        }
    }
}
