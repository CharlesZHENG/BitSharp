using BitSharp.Core.Storage;
using BitSharp.Core.Test.Storage;
using System;
using System.IO;

namespace BitSharp.Esent.Test
{
    public class EsentTestStorageProvider : ITestStorageProvider
    {
        private string baseDirectory;

        public string Name { get { return "Esent Storage"; } }

        public void TestInitialize()
        {
            this.baseDirectory = Path.Combine(Path.GetTempPath(), "BitSharp", "Tests");

            if (Directory.Exists(this.baseDirectory))
                Directory.Delete(this.baseDirectory, recursive: true);

            this.baseDirectory = Path.Combine(this.baseDirectory, Path.GetRandomFileName());
            Directory.CreateDirectory(this.baseDirectory);
        }

        public void TestCleanup()
        {
            if (Directory.Exists(this.baseDirectory))
            {
                try { Directory.Delete(this.baseDirectory, recursive: true); }
                catch (Exception) { }
            }
        }

        public IStorageManager OpenStorageManager()
        {
            return new EsentStorageManager(this.baseDirectory);
        }
    }
}
