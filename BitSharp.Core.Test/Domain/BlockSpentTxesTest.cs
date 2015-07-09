using BitSharp.Common;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BitSharp.Core.Test.Domain
{
    [TestClass]
    public class BlockSpentTxesTest
    {
        [TestMethod]
        public void TestBlockSpentTxes()
        {
            var spentTx0_0 = new SpentTx((UInt256)0, 0, 1);
            var spentTx1_0 = new SpentTx((UInt256)1, 1, 0);
            var spentTx1_1 = new SpentTx((UInt256)2, 1, 1);
            var spentTx2_0 = new SpentTx((UInt256)3, 2, 0);

            var builder = new BlockSpentTxesBuilder();
            builder.AddSpentTx(spentTx1_0);
            builder.AddSpentTx(spentTx2_0);
            builder.AddSpentTx(spentTx1_1);
            builder.AddSpentTx(spentTx0_0);

            var spentTxes = builder.ToImmutable();

            CollectionAssert.AreEqual(new[] { spentTx0_0, spentTx1_0, spentTx1_1, spentTx2_0 }, spentTxes.ToList());
        }
    }
}
