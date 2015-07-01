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

        private static void CreateDirectory(string path)
        {
            try
            {
                if (!Directory.Exists(path))
                    Directory.CreateDirectory(path);
            }
            catch (Exception) { }
        }

        private static void DeleteDirectory(string path)
        {
            try
            {
                if (Directory.Exists(path))
                    Directory.Delete(path, recursive: true);
            }
            catch (Exception) { }
        }

        private static void CleanCreateDirectory(string path)
        {
            DeleteDirectory(path);
            CreateDirectory(path);
        }

        private static bool IsProcessRunning(int processId)
        {
            try
            {
                using (Process.GetProcessById(processId))
                    return true;
            }
            catch (Exception)
            {
                return false;
            }
        }
    }
}
