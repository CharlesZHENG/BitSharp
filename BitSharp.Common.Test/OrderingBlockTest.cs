using BitSharp.Common.ExtensionMethods;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BitSharp.Common.Test
{
    [TestClass]
    public class OrderingBlockTest
    {
        [TestMethod]
        public void TestCaptureOrder()
        {
            // post source ints
            var intSource = new BufferBlock<int>();
            intSource.Post(1);
            intSource.Post(2);
            intSource.Post(99);
            intSource.Post(98);
            intSource.Complete();

            // capture source order
            var orderedInts = OrderingBlock.CaptureOrder<int, long, long>(
                intSource, intValue => (long)intValue);

            // post longs to combine, in reverse of original order
            var longSource = new BufferBlock<long>();
            longSource.Post(99);
            longSource.Post(98);
            longSource.Post(2);
            longSource.Post(1);
            longSource.Complete();

            // apply source order
            var orderedLongs = orderedInts.ApplyOrder(longSource, longValue => longValue);

            // verify the original order was preserved
            CollectionAssert.AreEqual(new long[] { 1, 2, 99, 98 }, orderedLongs.ToEnumerable().ToList());
        }
    }
}
