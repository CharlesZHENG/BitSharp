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
        public void TestInitialize()
        {
            BaseDirectory = Path.Combine(Path.GetTempPath(), "BitSharp", "Tests");
            try
            {
                if (Directory.Exists(BaseDirectory))
                    Directory.Delete(BaseDirectory, recursive: true);
            }
            catch (IOException) { }

            TestDirectory = Path.Combine(BaseDirectory, Path.GetRandomFileName());
            try
            {
                if (!Directory.Exists(TestDirectory))
                    Directory.CreateDirectory(TestDirectory);
            }
            catch (IOException) { }
        }

        public void TestCleanup()
        {
            try
            {
                if (Directory.Exists(BaseDirectory))
                    Directory.Delete(BaseDirectory, recursive: true);
            }
            catch (IOException) { }
        }

        public string BaseDirectory { get; private set; }

        public string TestDirectory { get; private set; }

        public abstract string Name { get; }

        public abstract IStorageManager OpenStorageManager();
    }
}
