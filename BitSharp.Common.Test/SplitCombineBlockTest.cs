using BitSharp.Common.ExtensionMethods;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BitSharp.Common.Test
{
    [TestClass]
    public class SplitCombineBlockTest
    {
        [TestMethod]
        public void TestSplitCombine()
        {
            TransformBlock<int, int> intSplitter; TransformManyBlock<long, long> longCombiner;
            SplitCombineBlock.Create<int, long, long>(
                intValue => (long)intValue,
                longValue => longValue,
                out intSplitter, out longCombiner);

            // post source ints
            var intSource = new BufferBlock<int>();
            intSource.Post(1);
            intSource.Post(2);
            intSource.Post(99);
            intSource.Post(98);
            intSource.Complete();

            intSource.LinkTo(intSplitter, new DataflowLinkOptions { PropagateCompletion = true });

            // post longs to combine, in reverse of original order
            longCombiner.Post(99);
            longCombiner.Post(98);
            longCombiner.Post(2);
            longCombiner.Post(1);
            longCombiner.Complete();

            // combine the longs and verify the original order was preserved
            using (var longQueue = longCombiner.LinkToQueue())
            {
                CollectionAssert.AreEqual(new long[] { 1, 2, 99, 98 }, longQueue.GetConsumingEnumerable().ToList());
            }
        }
    }
}
