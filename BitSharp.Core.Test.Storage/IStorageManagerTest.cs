using BitSharp.Core.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace BitSharp.Core.Test.Storage
{
    [TestClass]
    public class IStorageManagerTest : StorageProviderTest
    {
        [TestMethod]
        public void TestRollbackOfFailedChainStateCursor()
        {
            RunTest(TestRollbackOfFailedChainStateCursor);
        }

        private void TestRollbackOfFailedChainStateCursor(ITestStorageProvider provider)
        {
            using (var storageManager = provider.OpenStorageManager())
            {
                // begin a transaction on a cursor and return it without committing or rolling back
                IChainStateCursor chainStateCursor;
                using (var handle = storageManager.OpenChainStateCursor())
                {
                    chainStateCursor = handle.Item;
                    chainStateCursor.BeginTransaction();
                }

                using (var handle = storageManager.OpenChainStateCursor())
                {
                    // verify the same cursor was retrieved, ignore storage providers that do not re-use cursors
                    Assert.AreSame(handle.Item, chainStateCursor);

                    // verify the cursor is no longer in a transaction
                    chainStateCursor = handle.Item;
                    Assert.IsFalse(chainStateCursor.InTransaction);
                }
            }
        }
    }
}
