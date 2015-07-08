using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading;

namespace BitSharp.Common.Test
{
    [TestClass]
    public class TaskPoolTest
    {
        [TestMethod]
        public void TestTaskPool()
        {
            var runCount = 0;
            Action runAction = () => { Interlocked.Increment(ref runCount); };

            var expectedCount = 8;
            var resultTask = TaskPool.Run(expectedCount, runAction);
            resultTask.Wait();

            Assert.AreEqual(expectedCount, runCount);
        }
    }
}
