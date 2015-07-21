using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace BitSharp.Common.Test
{
    public static class AssertMethods
    {
        public static void AssertThrows<T>(Action action) where T : Exception
        {
            T ex;
            AssertThrows<T>(action, out ex);
        }

        public static void AssertThrows<T>(Action action, out T ex) where T : Exception
        {
            try
            {
                action();
                Assert.Fail($"No exception thrown, expected: {typeof(T).Name}");
                ex = null;
            }
            catch (UnitTestAssertException) { throw; }
            catch (Exception actualEx)
            {
                Assert.IsInstanceOfType(actualEx, typeof(T), $"Unexpected exeption thrown: {actualEx}");
                ex = (T)actualEx;
            }
        }

        public static void AssertAggregateThrows<T>(Action action) where T : Exception
        {
            T ex;
            AssertAggregateThrows<T>(action, out ex);
        }

        public static void AssertAggregateThrows<T>(Action action, out T ex) where T : Exception
        {
            AggregateException aggEx;
            AssertThrows<AggregateException>(action, out aggEx);

            var innerExceptions = aggEx.Flatten().InnerExceptions;
            Assert.AreEqual(1, innerExceptions.Count);
            Assert.IsInstanceOfType(innerExceptions[0], typeof(T), $"Unexpected exeption thrown: {innerExceptions[0]}");

            ex = (T)innerExceptions[0];
        }
    }
}
