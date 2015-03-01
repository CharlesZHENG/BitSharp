using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace BitSharp.Common.Test
{
    [TestClass]
    public class LookAheadTest
    {
        [TestMethod]
        public void TestReadException()
        {
            var expectedException = new Exception();
            try
            {
                foreach (var value in this.ThrowsEnumerable(expectedException).LookAhead(1))
                {
                }

                Assert.Fail("Expected exeption.");
            }
            catch (Exception actualException)
            {
                Assert.AreSame(expectedException, actualException);
            }
        }

        [TestMethod]
        public void TestConsumerException()
        {
            var expectedException = new Exception();
            using (var waitEvent = new ManualResetEventSlim())
            {
                try
                {
                    foreach (var value in Enumerable.Range(0, 1).LookAhead(1))
                    {
                        throw expectedException;
                    }

                    Assert.Fail("Expected exeption.");
                }
                catch (Exception actualException)
                {
                    Assert.AreSame(expectedException, actualException);
                }
            }
        }

        [TestMethod]
        [ExpectedException(typeof(OperationCanceledException))]
        public void TestCancelToken()
        {
            using (var cancelToken = new CancellationTokenSource())
            using (var waitEvent = new ManualResetEventSlim())
            {
                foreach (var value in this.WaitsEnumerable(waitEvent.WaitHandle).LookAhead(1, cancelToken.Token))
                {
                    cancelToken.Cancel();
                    waitEvent.Set();
                }
            }
        }

        private IEnumerable<object> ThrowsEnumerable(Exception e)
        {
            yield return new Object();
            yield return new Object();
            yield return new Object();
            yield return new Object();
            throw e;
        }

        private IEnumerable<object> WaitsEnumerable(WaitHandle waitHandle)
        {
            yield return new Object();
            yield return new Object();
            waitHandle.WaitOne();
            yield return new Object();
            yield return new Object();
            yield return new Object();
            yield return new Object();
            yield return new Object();
        }
    }
}
