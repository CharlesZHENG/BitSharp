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
        public void TestToEnumerable()
        {
            var expectedException = new Exception();

            var source = new BufferBlock<int>();
            source.Post(1);
            source.Post(2);
            source.Complete();

            var actual = source.ToEnumerable().ToList();
            CollectionAssert.AreEqual(new[] { 1, 2 }, actual);
        }

        [TestMethod]
        public void TestToEnumerableFault()
        {
            var expectedException = new Exception();

            var source = new BufferBlock<object>();
            ((IDataflowBlock)source).Fault(expectedException);

            try
            {
                source.ToEnumerable().ToList();
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
