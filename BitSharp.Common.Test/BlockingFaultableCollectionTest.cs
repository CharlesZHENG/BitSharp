using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;

namespace BitSharp.Common.Test
{
    [TestClass]
    public class BlockingFaultableCollectionTest
    {
        /// <summary>
        /// Test consuming the collection.
        /// </summary>
        [TestMethod]
        public void TestConsumingEnumerable()
        {
            var expectedException = new Exception();

            using (var collection = new BlockingFaultableCollection<object>())
            {
                collection.Add(1);
                collection.Add(2);
                collection.CompleteAdding();

                CollectionAssert.AreEqual(new[] { 1, 2 }, collection.GetConsumingEnumerable().ToList());
            }
        }

        /// <summary>
        /// Verify a fault on the collection is thrown when the collection is consumed.
        /// </summary>
        [TestMethod]
        public void TestConsumingFault()
        {
            var expectedException = new Exception();

            using (var collection = new BlockingFaultableCollection<object>())
            {
                collection.Fault(expectedException);

                try
                {
                    collection.GetConsumingEnumerable().ToList();
                    Assert.Fail();
                }
                catch (Exception ex)
                {
                    Assert.AreSame(expectedException, ex);
                }
            }
        }
    }
}
