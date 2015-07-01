using BitSharp.Core.Storage;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BitSharp.Core.Test.Storage
{
    public abstract class BaseTestStorageProvider : ITestStorageProvider
    {
        // cleanup the entire base directory on first initialization
        static BaseTestStorageProvider()
        {
            BaseDirectory = Path.Combine(Path.GetTempPath(), "BitSharp", "Tests");
            try
            {
                if (Directory.Exists(BaseDirectory))
                    Directory.Delete(BaseDirectory, recursive: true);
            }
            catch (IOException) { }
        }

        // create a random temp directory for this test instance
        public void TestInitialize()
        {
            TestDirectory = Path.Combine(BaseDirectory, Path.GetRandomFileName());
            try
            {
                if (!Directory.Exists(TestDirectory))
                    Directory.CreateDirectory(TestDirectory);
            }
            catch (IOException) { }
        }

        // cleanup the random temp directory for this test instance
        public void TestCleanup()
        {
            try
            {
                if (Directory.Exists(TestDirectory))
                    Directory.Delete(TestDirectory, recursive: true);
            }
            catch (IOException) { }
        }

        public static string BaseDirectory { get; private set; }

        public string TestDirectory { get; private set; }

        public abstract string Name { get; }

        public abstract IStorageManager OpenStorageManager();
    }
}
