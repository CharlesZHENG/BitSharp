using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace BitSharp.Core.Test
{
    public static class AssertMethods
    {
        public static void AssertDaemonAtBlock(int expectedHeight, UInt256 expectedBlockHash, CoreDaemon daemon)
        {
            Assert.AreEqual(expectedHeight, daemon.TargetChain.Height);
            Assert.AreEqual(expectedBlockHash, daemon.TargetChain.LastBlock.Hash);
            Assert.AreEqual(expectedHeight, daemon.CurrentChain.Height);
            Assert.AreEqual(expectedBlockHash, daemon.CurrentChain.LastBlock.Hash);
        }

        public static void AssertThrows<T>(Action action) where T : Exception
        {
            try
            {
                action();
                Assert.Fail("No exception thrown, expected: {0}".Format2(typeof(T).Name));
            }
            catch (UnitTestAssertException) { throw; }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(T), "Unexpected exeption thrown: {0}".Format2(e));
            }
        }
    }
}
