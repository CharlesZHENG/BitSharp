using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reactive.Disposables;
using System.Text;
using System.Threading.Tasks;

namespace BitSharp.Core.Test
{
    public class TempDirectory
    {
        public static string BaseDirectory { get; private set; }
        
        // cleanup the entire base directory on first initialization
        static TempDirectory()
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

        // create a random temp directory for this process
        public static string CreateTempDirectory()
        {
            var processDirectory = Path.Combine(BaseDirectory, Process.GetCurrentProcess().Id.ToString());
            CleanCreateDirectory(processDirectory);

            var testDirectory = Path.Combine(processDirectory, Path.GetRandomFileName());
            CleanCreateDirectory(testDirectory);

            return testDirectory;
        }

        public static IDisposable CreateTempDirectory(out string path)
        {
            var pathLocal = CreateTempDirectory();
            path = pathLocal;
            return Disposable.Create(() => DeleteDirectory(pathLocal));
        }

        public static void CreateDirectory(string path)
        {
            try
            {
                if (!Directory.Exists(path))
                    Directory.CreateDirectory(path);
            }
            catch (Exception) { }
        }

        public static void DeleteDirectory(string path)
        {
            try
            {
                if (Directory.Exists(path))
                    Directory.Delete(path, recursive: true);
            }
            catch (Exception) { }
        }

        public static void CleanCreateDirectory(string path)
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
