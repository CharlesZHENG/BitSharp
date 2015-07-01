using BitSharp.Core.Storage;
using System;
using System.Collections.Generic;
using System.Diagnostics;
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
            
            if (Directory.Exists(BaseDirectory))
            {
                // delete any subfolders, unless they are named with an active process id, which is another test currently in progress
                foreach (var subFolder in Directory.EnumerateDirectories(BaseDirectory))
                {
                    int processId;
                    var isOtherTestFolder = int.TryParse(subFolder, out processId) && IsProcessRunning(processId);

                    if (!isOtherTestFolder)
                        DeleteDirectory(subFolder);
                }
            }
            else
                CreateDirectory(BaseDirectory);
        }

        // create a random temp directory for this test instance
        public void TestInitialize()
        {
            ProcessDirectory = Path.Combine(BaseDirectory, Process.GetCurrentProcess().Id.ToString());
            CleanCreateDirectory(ProcessDirectory);

            TestDirectory = Path.Combine(ProcessDirectory, Path.GetRandomFileName());
            CleanCreateDirectory(ProcessDirectory);
        }

        // cleanup this processes random temp directory
        public void TestCleanup()
        {
            DeleteDirectory(ProcessDirectory);
        }

        public static string BaseDirectory { get; private set; }

        public string ProcessDirectory { get; private set; }

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
            catch (IOException) { }
        }

        private static void DeleteDirectory(string path)
        {
            try
            {
                if (Directory.Exists(path))
                    Directory.Delete(path, recursive: true);
            }
            catch (IOException) { }
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
