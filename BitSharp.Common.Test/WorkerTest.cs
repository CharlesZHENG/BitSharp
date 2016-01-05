using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Common.Test
{
    [TestClass]
    public class WorkerTest
    {
        [TestMethod]
        public void TestSubDispose()
        {
            // prepare subDispose call tracking
            var callCount = 0;
            Action subDispose = () => callCount++;

            // initialize worker
            using (var worker = new MockWorker(subDispose: subDispose))
            {
                // verify subDispose has not been called
                Assert.AreEqual(0, callCount);
            }

            // verify subDispose has been called after end of using
            Assert.AreEqual(1, callCount);
        }

        [TestMethod]
        public void TestSubStart()
        {
            // prepare subStart call tracking
            var callCount = 0;
            Action subStart = () => callCount++;

            // initialize worker
            using (var worker = new MockWorker(subStart: subStart))
            {
                // verify subStart has not been called
                Assert.AreEqual(0, callCount);

                // start the worker
                worker.Start();

                // verify subStart has been called
                Assert.AreEqual(1, callCount);
            }
        }

        [TestMethod]
        public void TestSubStop()
        {
            // prepare subStart call tracking
            var callCount = 0;
            Action subStop = () => callCount++;

            // initialize worker
            using (var worker = new MockWorker(subStop: subStop))
            {
                // verify subStop has not been called
                Assert.AreEqual(0, callCount);

                // stop the worker before it has been started
                worker.Stop();

                // verify subStop has not been called
                Assert.AreEqual(0, callCount);

                // start the worker
                worker.Start();

                // verify subStop has not been called
                Assert.AreEqual(0, callCount);

                // stop the worker
                worker.Stop();

                // verify subStop has been called
                Assert.AreEqual(1, callCount);
            }
        }

        [TestMethod]
        public void TestNotifyWork()
        {
            // prepare workAction call tracking
            var callEvent = new AutoResetEvent(false);
            Func<Task> workAction = () => { callEvent.Set(); return Task.CompletedTask; };

            // initialize worker
            using (var worker = new MockWorker(workAction))
            {
                // verify workAction has not been called
                var wasCalled = callEvent.WaitOne(1000);
                Assert.IsFalse(wasCalled);

                // start worker
                worker.Start();

                // verify workAction has not been called
                wasCalled = callEvent.WaitOne(1000);
                Assert.IsFalse(wasCalled);

                // notify worker
                worker.NotifyWork();

                // verify workAction has been called
                wasCalled = callEvent.WaitOne();
                Assert.IsTrue(wasCalled);

                // stop worker
                worker.Stop();

                // verify workAction has not been called
                wasCalled = callEvent.WaitOne(1000);
                Assert.IsFalse(wasCalled);
            }
        }

        [TestMethod]
        public void TestStartNotified()
        {
            // prepare workAction call tracking
            var callEvent = new AutoResetEvent(false);
            Func<Task> workAction = () => { callEvent.Set(); return Task.CompletedTask; };

            // initialize worker
            using (var worker = new MockWorker(workAction))
            {
                // verify workAction has not been called
                var wasCalled = callEvent.WaitOne(1000);
                Assert.IsFalse(wasCalled);

                // notify worker before it has been started
                worker.NotifyWork();

                // verify workAction has not been called
                wasCalled = callEvent.WaitOne(1000);
                Assert.IsFalse(wasCalled);

                // start worker, it has already been notified
                worker.Start();

                // verify workAction has been called
                wasCalled = callEvent.WaitOne();
                Assert.IsTrue(wasCalled);

                // stop worker
                worker.Stop();

                // verify workAction has not been called
                wasCalled = callEvent.WaitOne(1000);
                Assert.IsFalse(wasCalled);
            }
        }

        [TestMethod]
        public void TestRestart()
        {
            // prepare workAction call tracking
            var callEvent = new AutoResetEvent(false);
            Func<Task> workAction = () => { callEvent.Set(); return Task.CompletedTask; };

            // initialize worker
            using (var worker = new MockWorker(workAction))
            {
                // start worker
                worker.Start();

                // verify workAction has not been called
                var wasCalled = callEvent.WaitOne(1000);
                Assert.IsFalse(wasCalled);

                // notify worker
                worker.NotifyWork();

                // verify workAction has been called
                wasCalled = callEvent.WaitOne();
                Assert.IsTrue(wasCalled);

                // stop worker
                worker.Stop();

                // verify workAction has not been called
                wasCalled = callEvent.WaitOne(1000);
                Assert.IsFalse(wasCalled);

                // start worker again
                worker.Start();

                // notify worker
                worker.NotifyWork();

                // verify workAction has been called
                wasCalled = callEvent.WaitOne();
                Assert.IsTrue(wasCalled);
            }
        }

        [TestMethod]
        public void TestRestartNotified()
        {
            // prepare workAction call tracking
            var callEvent = new AutoResetEvent(false);
            Func<Task> workAction = () => { callEvent.Set(); return Task.CompletedTask; };

            // initialize worker
            using (var worker = new MockWorker(workAction))
            {
                // start worker
                worker.Start();

                // verify workAction has not been called
                var wasCalled = callEvent.WaitOne(1000);
                Assert.IsFalse(wasCalled);

                // notify worker
                worker.NotifyWork();

                // verify workAction has been called
                wasCalled = callEvent.WaitOne();
                Assert.IsTrue(wasCalled);

                // stop worker
                worker.Stop();

                // notify worker again
                worker.NotifyWork();

                // verify workAction has not been called
                wasCalled = callEvent.WaitOne(1000);
                Assert.IsFalse(wasCalled);

                // start worker again, it has already been notified
                worker.Start();

                // verify workAction has been called
                wasCalled = callEvent.WaitOne(5000);
                Assert.IsTrue(wasCalled);
            }
        }

        [TestMethod]
        public void TestWorkAfterCancellation()
        {
            // create a work action that will throw a cancellation exception on its first call
            var workedEvent = new AutoResetEvent(false);
            var workCount = 0;
            Func<Task> workAction = () =>
                {
                    try
                    {
                        workCount++;
                        if (workCount == 1)
                            throw new OperationCanceledException();

                        return Task.CompletedTask;
                    }
                    finally
                    {
                        workedEvent.Set();
                    }
                };

            // initialize worker
            using (var worker = new MockWorker(workAction))
            {
                worker.Start();

                // notify and verify work was performed
                worker.NotifyWork();
                Assert.IsTrue(workedEvent.WaitOne());
                Assert.AreEqual(1, workCount);

                // the first work action cancelled, need to verify that a second work action can be performed

                // notify and verify work was performed
                worker.NotifyWork();
                Assert.IsTrue(workedEvent.WaitOne());
                Assert.AreEqual(2, workCount);
            }
        }

        [TestMethod]
        public void TestOnNotifyWork()
        {
            Assert.Inconclusive("TODO");
        }

        [TestMethod]
        public void TestOnWorkStarted()
        {
            Assert.Inconclusive("TODO");
        }

        [TestMethod]
        public void TestOnWorkFinished()
        {
            Assert.Inconclusive("TODO");
        }

        [TestMethod]
        public void TestOnWorkError()
        {
            // prepare workAction to throw exception
            var expectedException = new Exception();
            Func<Task> workAction = () => { throw expectedException; };

            // initialize worker
            using (var worker = new MockWorker(workAction))
            {
                // prepare OnWorkError call tracking
                Exception actualException = null;
                var callEvent = new AutoResetEvent(false);
                worker.OnWorkError += e => { actualException = e; callEvent.Set(); };

                // start worker
                worker.Start();

                // notify worker
                worker.NotifyWork();

                // verify OnWorkError was called with expected exception
                var wasCalled = callEvent.WaitOne();
                Assert.IsTrue(wasCalled);
                Assert.AreSame(expectedException, actualException);
            }
        }

        [TestMethod]
        public void TestWorkCancelledException()
        {
            // prepare workAction to throw exception
            Exception currentException = null;
            Func<Task> workAction = () => { throw currentException; };

            // initialize worker
            using (var worker = new MockWorker(workAction))
            {
                var finishedEvent = new AutoResetEvent(false);
                worker.OnWorkFinished += () => { finishedEvent.Set(); };

                bool wasError;
                worker.OnWorkError += e => wasError = true;

                // start worker
                worker.Start();

                // throw OperationCanceledException
                wasError = false;
                currentException = new OperationCanceledException();
                worker.NotifyWork();

                // verify work finished
                Assert.IsTrue(finishedEvent.WaitOne(1000));
                Assert.IsFalse(wasError);

                // throw Exception
                wasError = false;
                currentException = new Exception();
                worker.NotifyWork();

                // verify work errored
                Assert.IsTrue(finishedEvent.WaitOne());
                Assert.IsTrue(wasError);

                // throw AggregateException of all OperationCanceledException
                wasError = false;
                currentException = new AggregateException(new OperationCanceledException(), new OperationCanceledException());
                worker.NotifyWork();

                // verify work finished
                Assert.IsTrue(finishedEvent.WaitOne());
                Assert.IsFalse(wasError);

                // throw AggregateException of some OperationCanceledException
                wasError = false;
                currentException = new AggregateException(new OperationCanceledException(), new Exception());
                worker.NotifyWork();

                // verify work errored
                Assert.IsTrue(finishedEvent.WaitOne());
                Assert.IsTrue(wasError);
            }
        }
    }
}
