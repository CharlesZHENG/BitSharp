
using BitSharp.Core.Storage;

namespace BitSharp.Core.Test.Storage
{
    public interface ITestStorageProvider
    {
        string Name { get; }

        void TestInitialize();

        void TestCleanup();

        IStorageManager OpenStorageManager();
    }
}
