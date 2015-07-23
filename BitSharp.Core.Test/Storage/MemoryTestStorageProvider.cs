using BitSharp.Core.Storage;
using BitSharp.Core.Storage.Memory;

namespace BitSharp.Core.Test.Storage
{
    public class MemoryTestStorageProvider : ITestStorageProvider
    {
        public string Name => "Memory Storage";

        public void TestInitialize() { }

        public void TestCleanup() { }

        public IStorageManager OpenStorageManager()
        {
            return new MemoryStorageManager();
        }
    }
}
