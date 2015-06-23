using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Common.Test
{
    [TestClass]
    public class ParallelReaderTest
    {
        /// <summary>
        /// Verify that ParallelReader doesn't hang on disposal when a pending read is outstanding.
        /// </summary>
        [TestMethod]
        [Timeout(2000)]
        public void TestUnconsumedDisposal()
        {
            using (var reader = new ParallelReader<int>(""))
            {
                // begin reading, without consuming
                var readerTask = reader.ReadAsync(Enumerable.Range(0, 10));
            }
        }

        /// <summary>
        /// Verify that reads occurr in parallel, and the sequence of task completions.
        /// </summary>
        [TestMethod]
        public void TestReadTaskSequence()
        {
            using (var reader = new ParallelReader<int>(""))
            {
                // create a blocking source, not yet completed
                var source = new BlockingCollection<int> { 1, 2, 3 };

                // begin reading
                Task readsQueuedTask;
                var readerTask = reader.ReadAsync(source.GetConsumingEnumerable(), null, null, out readsQueuedTask);

                // verify reader hasn't completed, consumption hasn't started
                Assert.IsFalse(readerTask.Wait(25));

                // verify all items have been read from source, before consumption has started
                Assert.AreEqual(0, source.Count); //TODO this assumes that readerTask.Wait() is enough time to read the source

                // verify queuing is not finished, source has not been completed yet
                Assert.IsFalse(readsQueuedTask.IsCompleted);

                // complete the source and verify the queuing task finishes
                source.CompleteAdding();
                Assert.IsTrue(readsQueuedTask.Wait(2000));

                // create a task to read the results
                var results = new List<int>();
                var consumeTask = Task.Run(() => results.AddRange(reader.GetConsumingEnumerable()));

                // wait for results to be read
                Assert.IsTrue(consumeTask.Wait(2000));

                // verify the results were read correctly from source
                CollectionAssert.AreEqual(new[] { 1, 2, 3 }, results);

                // verify reader task completes
                Assert.IsTrue(readerTask.Wait(2000));
            }
        }

        /// <summary>
        /// Verify multiple consumers running simultaneously.
        /// </summary>
        [TestMethod]
        public void TestMultipleConsumers()
        {
            using (var reader = new ParallelReader<int>(""))
            using (var consume2Started = new ManualResetEventSlim())
            {
                // create a blocking source, not yet completed
                var source = new BlockingCollection<int> { 1, 2, 3 };
                source.CompleteAdding();

                // begin reading
                var readerTask = reader.ReadAsync(source.GetConsumingEnumerable());

                // create two tasks to read the results
                var results = new ConcurrentQueue<int>();

                // task 1 will wait for task 2 to read one item, and then read the rest
                var consumeTask1 = Task.Run(() =>
                {
                    // wait for task 2 to read one item
                    consume2Started.Wait();

                    // read the remaining items
                    foreach (var item in reader.GetConsumingEnumerable())
                        results.Enqueue(item);
                });

                // task 2 will read 1 item, and then allow task 1 to read the rest
                var consumeTask2 = Task.Run(() =>
                {
                    foreach (var item in reader.GetConsumingEnumerable())
                    {
                        results.Enqueue(item);

                        // notify that task 2 has read one item
                        consume2Started.Set();

                        // wait for task 1 to consume all remaining items
                        consumeTask1.Wait();
                    }
                });

                Task.WaitAll(consumeTask1, consumeTask2);

                // verify the results were read correctly from source
                CollectionAssert.AreEquivalent(new[] { 1, 2, 3 }, results);
            }
        }

        /// <summary>
        /// Verify that a read can be cancelled while consumption is ongoing.
        /// </summary>
        [TestMethod]
        public void TestCancelDuringConsume()
        {
            using (var reader = new ParallelReader<int>(""))
            using (var cancelToken = new CancellationTokenSource())
            using (var consumeEvent = new AutoResetEvent(false))
            using (var consumedEvent = new AutoResetEvent(false))
            {
                // create blocking source
                var source = new BlockingCollection<int> { 1, 2, 3 };
                source.CompleteAdding();

                // begin reading
                var readerTask = reader.ReadAsync(source.GetConsumingEnumerable(), cancelToken: cancelToken.Token);

                // create a task to consume single items as events are signalled
                var results = new ConcurrentQueue<int>();
                var consumeTask = Task.Run(() =>
                {
                    // wait for initial consume event
                    consumeEvent.WaitOne();
                    foreach (var item in reader.GetConsumingEnumerable())
                    {
                        results.Enqueue(item);

                        // notify an item has been consumed
                        consumedEvent.Set();

                        // wait for another consume event
                        consumeEvent.WaitOne();
                    }
                });

                // allow 1 item to be consumed
                consumeEvent.Set();

                // wait for consume and verify
                consumedEvent.WaitOne();
                CollectionAssert.AreEquivalent(new[] { 1 }, results);

                // cancel the reader
                cancelToken.Cancel();

                // allow consumption to resume
                consumeEvent.Set();

                // verify the consume task received an operation cancelled exception
                try
                {
                    consumeTask.Wait(2000);
                    Assert.Fail("Cancellation exception not thrown.");
                }
                catch (AggregateException ex)
                {
                    Assert.IsTrue(ex.InnerExceptions[0] is OperationCanceledException);
                    Assert.IsTrue(consumeTask.IsFaulted);
                }

                // verify that no additional elements were consumed
                CollectionAssert.AreEquivalent(new[] { 1 }, results);

                // verify the read task was cancelled
                try
                {
                    readerTask.Wait(2000);
                    Assert.Fail("Cancellation exception not thrown.");
                }
                catch (AggregateException ex)
                {
                    Assert.IsTrue(ex.InnerExceptions[0] is TaskCanceledException);
                    Assert.IsTrue(readerTask.IsCanceled);
                }
            }
        }

        /// <summary>
        /// Verify that a read can be cancelled if consumption is never started.
        /// </summary>
        [TestMethod]
        public void TestCancelWithoutConsume()
        {
            using (var reader = new ParallelReader<int>(""))
            using (var cancelToken = new CancellationTokenSource())
            using (var consumeEvent = new AutoResetEvent(false))
            using (var consumedEvent = new AutoResetEvent(false))
            {
                // create blocking source
                var source = new BlockingCollection<int> { 1, 2, 3 };
                source.CompleteAdding();

                // begin reading
                var readerTask = reader.ReadAsync(source.GetConsumingEnumerable(), cancelToken: cancelToken.Token);

                // cancel the reader
                cancelToken.Cancel();

                // verify the read task was cancelled
                try
                {
                    readerTask.Wait(2000);
                    Assert.Fail("Cancellation exception not thrown.");
                }
                catch (AggregateException ex)
                {
                    Assert.IsTrue(ex.InnerExceptions[0] is TaskCanceledException);
                    Assert.IsTrue(readerTask.IsCanceled);
                }
            }
        }

        /// <summary>
        /// Verify that an exception in the source enumerable is thrown and received by all tasks.
        /// </summary>
        [TestMethod]
        public void TestSourceException()
        {
            using (var reader = new ParallelReader<int>(""))
            using (var consumeEvent = new AutoResetEvent(false))
            using (var consumedEvent = new AutoResetEvent(false))
            {
                // create a source that will throws exceptions
                var zero = 0;
                var source = Enumerable.Range(0, 10).Select(x => 1 / zero);

                // begin reading
                Task readsQueuedTask;
                var readerTask = reader.ReadAsync(source, null, null, out readsQueuedTask);

                // begin consuming
                var results = new List<int>();
                var consumeTask = Task.Run(() => results.AddRange(reader.GetConsumingEnumerable()));

                // verify the consume task received the source exception
                try
                {
                    consumeTask.Wait(2000);
                    Assert.Fail("Source exception not thrown.");
                }
                catch (AggregateException ex)
                {
                    Assert.IsTrue(ex.InnerExceptions[0] is DivideByZeroException);
                    Assert.IsTrue(consumeTask.IsFaulted);
                }

                // verify the reads queued task received the source exception
                try
                {
                    readsQueuedTask.Wait(2000);
                    Assert.Fail("Source exception not thrown.");
                }
                catch (AggregateException ex)
                {
                    Assert.IsTrue(ex.InnerExceptions[0] is DivideByZeroException);
                    Assert.IsTrue(readsQueuedTask.IsFaulted);
                }

                // verify the read task received the source exception
                try
                {
                    readerTask.Wait(2000);
                    Assert.Fail("Source exception not thrown.");
                }
                catch (AggregateException ex)
                {
                    Assert.IsTrue(ex.InnerExceptions[0] is DivideByZeroException);
                    Assert.IsTrue(readerTask.IsFaulted);
                }
            }
        }

        [TestMethod]
        public void TestConsumePartial()
        {
            using (var reader = new ParallelReader<int>(""))
            {
                var source = Enumerable.Range(0, 10);

                // begin reading
                var readerTask = reader.ReadAsync(source);

                // consume half
                Assert.AreEqual(5, reader.GetConsumingEnumerable().Take(5).Count());

                // consume remaining half
                Assert.AreEqual(5, reader.GetConsumingEnumerable().Count());

                // wait for read task to complete
                Assert.IsTrue(readerTask.Wait(2000));
            }
        }

        [TestMethod]
        public void TestConsumeAfterComplete()
        {
            using (var reader = new ParallelReader<int>(""))
            {
                var source = Enumerable.Range(0, 10);

                // begin reading
                var readerTask = reader.ReadAsync(source);

                // consume all
                reader.GetConsumingEnumerable().Count();

                // wait for read task to complete
                Assert.IsTrue(readerTask.Wait(2000));

                // try to consume after task is complete, should not cause an error
                Assert.AreEqual(0, reader.GetConsumingEnumerable().Count());
            }
        }
    }
}
