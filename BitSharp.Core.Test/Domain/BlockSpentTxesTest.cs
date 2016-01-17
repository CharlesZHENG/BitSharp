using BitSharp.Common;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Core.Test.Domain
{
    [TestClass]
    public class BlockSpentTxesTest
    {
        [TestMethod]
        public void TestBlockSpentTxes()
        {
            var spentTx0_0 = new SpentTx((UInt256)0, 0, 1, 0);
            var spentTx1_0 = new SpentTx((UInt256)1, 1, 0, 0);
            var spentTx1_1 = new SpentTx((UInt256)2, 1, 1, 0);
            var spentTx2_0 = new SpentTx((UInt256)3, 2, 0, 0);

            var builder = new BlockSpentTxesBuilder();
            builder.AddSpentTx(spentTx1_0);
            builder.AddSpentTx(spentTx2_0);
            builder.AddSpentTx(spentTx1_1);
            builder.AddSpentTx(spentTx0_0);

            var spentTxes = builder.ToImmutable();

            CollectionAssert.AreEqual(new[] { spentTx0_0, spentTx1_0, spentTx1_1, spentTx2_0 }, spentTxes.ToList());
        }

        [TestMethod]
        public void TestReadByBlock()
        {
            var spentTx0_0 = new SpentTx((UInt256)0, 0, 1, 0);
            var spentTx1_0 = new SpentTx((UInt256)1, 1, 0, 0);
            var spentTx1_1 = new SpentTx((UInt256)2, 1, 1, 0);
            var spentTx2_0 = new SpentTx((UInt256)3, 2, 0, 0);

            var builder = new BlockSpentTxesBuilder();
            builder.AddSpentTx(spentTx1_0);
            builder.AddSpentTx(spentTx2_0);
            builder.AddSpentTx(spentTx1_1);
            builder.AddSpentTx(spentTx0_0);

            var spentTxes = builder.ToImmutable();

            var expectedByBlock = new[]
            {
                Tuple.Create(0, (IImmutableList<SpentTx>)ImmutableList.Create(spentTx0_0)),
                Tuple.Create(1, (IImmutableList<SpentTx>)ImmutableList.Create(spentTx1_0, spentTx1_1)),
                Tuple.Create(2, (IImmutableList<SpentTx>)ImmutableList.Create(spentTx2_0)),
            }.ToList();

            var actualByBlock = spentTxes.ReadByBlock().ToList();

            Assert.AreEqual(expectedByBlock.Count, actualByBlock.Count);
            for (var i = 0; i < actualByBlock.Count; i++)
            {
                Assert.AreEqual(expectedByBlock[i].Item1, actualByBlock[i].Item1);
                CollectionAssert.AreEqual(expectedByBlock[i].Item2.ToList(), actualByBlock[i].Item2.ToList());
            }
        }
    }
}
