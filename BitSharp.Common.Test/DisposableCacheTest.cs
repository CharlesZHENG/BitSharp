using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Common.Test
{
    [TestClass]
    public class DisposableCacheTest
    {
        // verify taking an item from the cache
        [TestMethod]
        public void TestTakeItem()
        {
            var openCount = 0;
            var disposeCount = 0;

            Func<IDisposable> createFunc =
                () =>
                {
                    var disposable = new Mock<IDisposable>();

                    disposable.Setup(x => x.Dispose())
                        .Callback(() => disposeCount++);

                    openCount++;
                    return disposable.Object;
                };

            // create a cache with a capacity of 2
            using (var cache = new DisposableCache<IDisposable>(2, createFunc))
            {
                Assert.AreEqual(0, openCount);
                Assert.AreEqual(0, disposeCount);

                // take 3 items, 1 should be disposed when it is returned
                using (var handle1 = cache.TakeItem())
                {
                    Assert.AreEqual(1, openCount);
                    Assert.AreEqual(0, disposeCount);

                    using (var handle2 = cache.TakeItem())
                    {
                        Assert.AreEqual(2, openCount);
                        Assert.AreEqual(0, disposeCount);

                        using (var handle3 = cache.TakeItem())
                        {
                            Assert.AreEqual(3, openCount);
                            Assert.AreEqual(0, disposeCount);
                        }

                        Assert.AreEqual(3, openCount);
                        Assert.AreEqual(0, disposeCount);
                    }

                    Assert.AreEqual(3, openCount);
                    Assert.AreEqual(0, disposeCount);
                }

                // verify final item was disposed as the cache was full
                Assert.AreEqual(3, openCount);
                Assert.AreEqual(1, disposeCount);
            }

            // now that cache is disposed, remaining two items should have been disposed
            Assert.AreEqual(3, openCount);
            Assert.AreEqual(3, disposeCount);
        }

        // verify taking an item from an exhausted cache waits for a freed item
        [TestMethod]
        public void TestTakeItemExhausted()
        {
            using (var cache = new DisposableCache<IDisposable>(1))
            using (var taker1Taken = new AutoResetEvent(false))
            using (var taker1Finish = new AutoResetEvent(false))
            using (var taker2Started = new AutoResetEvent(false))
            using (var taker2Taken = new AutoResetEvent(false))
            {
                var item = new Mock<IDisposable>().Object;
                cache.CacheItem(item);

                var taker1 = Task.Run(() =>
                    {
                        using (var handle1 = cache.TakeItem())
                        {
                            taker1Taken.Set();
                            taker1Finish.WaitOne();
                        }
                    });

                // wait for taker 1 to retrieve a cached item
                taker1Taken.WaitOne();

                var taker2 = Task.Run(() =>
                    {
                        taker2Started.Set();
                        using (var handle2 = cache.TakeItem())
                        {
                            taker2Taken.Set();
                        }
                    });

                // wait for taker 2 to start
                Assert.IsTrue(taker2Started.WaitOne(2000));

                // verify taker 2 hasn't been able to retrieve a cached item
                Assert.IsFalse(taker2Taken.WaitOne(500));

                // allow taker 1 to return its cached item
                taker1Finish.Set();
                Assert.IsTrue(taker1.Wait(2000));

                // verify taker 2 then retrieves a cached item
                Assert.IsTrue(taker2Taken.WaitOne(2000));
                Assert.IsTrue(taker2.Wait(2000));
            }
        }

        // verify taking an item from an exhausted cache times out properly
        [TestMethod]
        public void TestTakeItemTimeout()
        {
            using (var cache = new DisposableCache<IDisposable>(1))
            {
                var item = new Mock<IDisposable>().Object;
                cache.CacheItem(item);

                using (var handle1 = cache.TakeItem())
                {
                    var timeout = TimeSpan.FromMilliseconds(250);

                    var stopwatch = Stopwatch.StartNew();
                    try
                    {
                        using (var handle2 = cache.TakeItem(timeout))
                        { }
                        Assert.Fail();
                    }
                    catch (TimeoutException) { /*expected*/ }
                    stopwatch.Stop();

                    // verify the timout period was respected before the exception was thrown
                    Assert.IsTrue(stopwatch.Elapsed >= timeout);
                }
            }
        }

        // verify taking an item from an exhausted cache when an item is returned while waiting
        [TestMethod]
        public void TestTakeItemTimeoutFreed()
        {
            using (var cache = new DisposableCache<IDisposable>(1))
            {
                var item = new Mock<IDisposable>().Object;
                cache.CacheItem(item);

                var delay = TimeSpan.FromMilliseconds(50);
                var timeout = TimeSpan.FromMilliseconds(5000);

                // return the first item after a delay
                var handle1 = cache.TakeItem();
                Task.Run(() =>
                {
                    using (handle1)
                    {
                        Task.Delay(delay).Wait();
                    }
                });

                // take a second item and verify it succeeds after the delay has passed
                var stopwatch = Stopwatch.StartNew();
                using (var handle2 = cache.TakeItem(timeout))
                {
                    stopwatch.Stop();

                    Assert.IsTrue(stopwatch.Elapsed >= delay);
                }
            }
        }
    }
}
