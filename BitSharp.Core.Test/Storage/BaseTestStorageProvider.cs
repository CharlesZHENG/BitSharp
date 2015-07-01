using BitSharp.Core.Storage;
using System;
using System.Diagnostics;
using System.IO;

namespace BitSharp.Core.Test.Storage
{
    public abstract class BaseTestStorageProvider : ITestStorageProvider
    {
        // create a random temp directory for this test instance
        public void TestInitialize()
        {
            TestDirectory = TempDirectory.CreateTempDirectory();
        }

        // cleanup this processes random temp directory
        public void TestCleanup()
        {
            TempDirectory.DeleteDirectory(TestDirectory);
        }

        public string TestDirectory { get; private set; }

        public abstract string Name { get; }

        public abstract IStorageManager OpenStorageManager();
    }
}
