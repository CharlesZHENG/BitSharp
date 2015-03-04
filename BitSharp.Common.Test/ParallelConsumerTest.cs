using Microsoft.VisualStudio.TestTools.UnitTesting;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Linq;

namespace BitSharp.Common.Test
{
    [TestClass]
    public class ParallelConsumerTest
    {
        [TestMethod]
        public void TestConsume()
        {
            using (var consumer = new ParallelConsumer<int>("", 4))
            {
                // create a source enumerable
                var source = Enumerable.Range(0, 5).Select(x => x);

                // create a consume action
                var expectedResults = Enumerable.Range(0, 5).Select(x => x * 10).ToList();
                var results = new ConcurrentBag<int>();
                Action<int> consumeAction = x =>
                    {
                        results.Add(x * 10);
                    };

                // start consuming the source
                using (consumer.Start(source, consumeAction, () => { }))
                {
                    // wait for the consumer to complete
                    consumer.WaitToComplete();

                    // verify results
                    CollectionAssert.AreEquivalent(expectedResults, results);
                }
            }
        }

        [TestMethod]
        public void TestCompletedAction()
        {
            using (var consumer = new ParallelConsumer<int>("", 4))
            {
                // create a source enumerable
                var source = Enumerable.Range(0, 5).Select(x => x);

                // create a completed action
                var wasCompleted = false;
                Action completedAction = () => wasCompleted = true;

                // start consuming the source
                using (consumer.Start(source, _ => { }, completedAction))
                {
                    // wait for the consumer to complete
                    consumer.WaitToComplete();

                    // verify completed action was called
                    Assert.IsTrue(wasCompleted);
                }
            }
        }

        [TestMethod]
        public void TestSourceException()
        {
            using (var consumer = new ParallelConsumer<int>("", 4))
            {
                // create a source enumerable that will throw an exception
                var expectedException = new Exception();
                var source = Enumerable.Range(0, 5).Select(
                    x =>
                    {
                        if (x < 4)
                            return x;
                        else
                            throw expectedException;
                    });

                // create a completed action
                var wasCompleted = false;
                Action completedAction = () => wasCompleted = true;

                // start consuming the source
                using (consumer.Start(source, _ => { }, completedAction))
                {
                    // wait for the consumer to complete, and verify the exception was bubbled up
                    try
                    {
                        consumer.WaitToComplete();
                        Assert.Fail("Expected exception was not thrown.");
                    }
                    catch (AggregateException e)
                    {
                        // verify expected exception
                        Assert.AreEqual(1, e.InnerExceptions.Count);
                        Assert.AreSame(expectedException, e.InnerExceptions[0]);

                        // verify completed action was not called
                        Assert.IsFalse(wasCompleted);
                    }
                }
            }
        }

        [TestMethod]
        public void TestConsumerException()
        {
            using (var consumer = new ParallelConsumer<int>("", 4))
            {
                // create a source enumerable
                var source = Enumerable.Range(0, 5).Select(x => x);

                // create a consume action that will throw an exception on the last item
                var expectedException = new Exception();
                Action<int> consumeAction = x =>
                    {
                        if (x == 4)
                            throw expectedException;
                    };

                // create a completed action
                var wasCompleted = false;
                Action completedAction = () => wasCompleted = true;

                // start consuming the source
                using (consumer.Start(source, consumeAction, completedAction))
                {
                    // wait for the consumer to complete, and verify the exception was bubbled up
                    try
                    {
                        consumer.WaitToComplete();
                        Assert.Fail("Expected exception was not thrown.");
                    }
                    catch (AggregateException e)
                    {
                        // verify expected exception
                        Assert.AreEqual(1, e.InnerExceptions.Count);
                        Assert.AreSame(expectedException, e.InnerExceptions[0]);

                        // verify completed action was not called
                        Assert.IsFalse(wasCompleted);
                    }
                }
            }
        }
    }
}
