using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;

namespace BitSharp.Common.Test
{
    [TestClass]
    public class ConcurrentBlockingQueueTest
    {
        [TestMethod]
        public void TestMultipleConsumers()
        {
            using (var queue = new ConcurrentBlockingQueue<int>())
            {
                var count = 10;
                var tasks = new Task[count];
                var results = new ConcurrentBag<int>();

                // create multiple tasks that will each read one item from the queue
                for (var i = 0; i < count; i++)
                {
                    tasks[i] = Task.Run(() =>
                        {
                            foreach (var item in queue.GetConsumingEnumerable())
                            {
                                results.Add(item);
                                return;
                            }
                        });
                }

                // add items to the queue
                for (var i = 0; i < count; i++)
                    queue.Add(i);

                // complete adding
                queue.CompleteAdding();

                // wait for read tasks
                Task.WaitAll(tasks);

                // verify results
                CollectionAssert.AreEquivalent(Enumerable.Range(0, count).ToArray(), results.ToArray());
            }
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void TestAddAfterComplete()
        {
            using (var queue = new ConcurrentBlockingQueue<int>())
            {
                queue.Add(1);
                queue.CompleteAdding();

                // verify an item can't be added after completing, this should throw
                queue.Add(2);
            }
        }

        [TestMethod]
        public void TestConsumeAfterComplete()
        {
            using (var queue = new ConcurrentBlockingQueue<int>())
            {
                queue.Add(1);
                queue.Add(2);
                queue.CompleteAdding();

                // read all items
                Assert.AreEqual(2, queue.GetConsumingEnumerable().Count());

                // read again after completed adding and consuming
                Assert.AreEqual(0, queue.GetConsumingEnumerable().Count());
            }
        }

        [TestMethod]
        public void TestConsumerException()
        {
            using (var queue = new ConcurrentBlockingQueue<int>())
            {
                // create a task to read one item and stop
                queue.Add(1);
                var task1 = Task.Run(() =>
                    {
                        foreach (var item in queue.GetConsumingEnumerable())
                            return;
                    });

                // create a task to read one item and throw
                queue.Add(2);
                var expectedException = new Exception();
                var task2 = Task.Run(() =>
                    {
                        foreach (var item in queue.GetConsumingEnumerable())
                            throw expectedException;
                    });

                // wait for successful read
                task1.Wait();

                // wait for error read, and verify the thrown exception was bubbled up
                try
                {
                    task2.Wait();
                    Assert.Fail();
                }
                catch (AggregateException e)
                {
                    Assert.AreEqual(1, e.InnerExceptions.Count);
                    Assert.AreSame(expectedException, e.InnerExceptions[0]);
                }

                // add another item after the error
                queue.Add(3);

                // verify queue still completes successfully
                queue.CompleteAdding();
                Assert.AreEqual(1, queue.GetConsumingEnumerable().Count());
            }
        }
    }
}
