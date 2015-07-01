using BitSharp.Common.ExtensionMethods;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;
using System.Threading.Tasks.Dataflow;

namespace BitSharp.Common.Test
{
    [TestClass]
    public class DataflowExtensionMethodsTest
    {
        [TestMethod]
        public void TestLinkToQueue()
        {
            var expectedException = new Exception();

            var source = new BufferBlock<int>();
            source.Post(1);
            source.Post(2);
            source.Complete();

            using (var queue = source.LinkToQueue())
            {
                var actual = queue.GetConsumingEnumerable().ToList();
                CollectionAssert.AreEqual(new[] { 1, 2 }, actual);
            }
        }

        [TestMethod]
        public void TestLinkToQueueFault()
        {
            var expectedException = new Exception();

            var source = new BufferBlock<object>();
            ((IDataflowBlock)source).Fault(expectedException);

            using (var queue = source.LinkToQueue())
            {
                try
                {
                    queue.GetConsumingEnumerable().ToList();
                    Assert.Fail();
                }
                catch (Exception ex)
                {
                    Assert.IsInstanceOfType(ex, typeof(AggregateException));
                    var aggEx = (AggregateException)ex;
                    Assert.AreEqual(1, aggEx.Flatten().InnerExceptions.Count);
                    Assert.AreSame(expectedException, aggEx.Flatten().InnerExceptions[0]);
                }
            }
        }
    }
}
