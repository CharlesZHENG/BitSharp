using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage;
using LightningDB;
using NLog;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace BitSharp.Lmdb
{
    public class LmdbStorageManager : IStorageManager
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly string baseDirectory;
        private readonly string[] blockTxesStorageLocations;
        private readonly long blocksSize;
        private readonly long blockTxesSize;
        private readonly long chainStateSize;

        private readonly object blockStorageLock;
        private readonly object blockTxesStorageLock;
        private readonly object chainStateManagerLock;

        private LmdbBlockStorage blockStorage;
        private IBlockTxesStorage blockTxesStorage;
        private LmdbChainStateManager chainStateManager;

        private bool isDisposed;

        public LmdbStorageManager(string baseDirectory, string[] blockTxesStorageLocations = null)
        {
            this.baseDirectory = baseDirectory;
            this.blockTxesStorageLocations = blockTxesStorageLocations;

            if (Environment.Is64BitProcess)
            {
                this.blocksSize = 4.BILLION();
                this.blockTxesSize = 128.BILLION();
                this.chainStateSize = 32.BILLION();
            }
            else
            {
                this.blocksSize = 100.MILLION();
                this.blockTxesSize = 100.MILLION();
                this.chainStateSize = 100.MILLION();
            }

            this.blockStorageLock = new object();
            this.blockTxesStorageLock = new object();
            this.chainStateManagerLock = new object();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!isDisposed && disposing)
            {
                if (this.chainStateManager != null)
                    this.chainStateManager.Dispose();

                if (this.blockStorage != null)
                    this.blockStorage.Dispose();

                if (this.blockTxesStorage != null)
                    this.blockTxesStorage.Dispose();

                isDisposed = true;
            }
        }

        public IBlockStorage BlockStorage
        {
            get
            {
                if (this.blockStorage == null)
                    lock (this.blockStorageLock)
                        if (this.blockStorage == null)
                            this.blockStorage = new LmdbBlockStorage(this.baseDirectory, this.blocksSize);

                return this.blockStorage;
            }
        }

        public IBlockTxesStorage BlockTxesStorage
        {
            get
            {
                const int splitCount = 32;
                if (this.blockTxesStorage == null)
                    lock (this.blockTxesStorageLock)
                        if (this.blockTxesStorage == null)
                        {
                            if (blockTxesStorageLocations == null)
                            {
                                // split LMDB storage since only one writer is allowed at a time
                                this.blockTxesStorage = new SplitBlockTxesStorage(splitCount, x => new LmdbBlockTxesStorage(this.baseDirectory, this.blockTxesSize / splitCount, x));
                            }
                            else
                            {
                                // LMDB storage should still be further split up within each split location, since only one writer is allowed at a time
                                this.blockTxesStorage = new SplitBlockTxesStorage(blockTxesStorageLocations, path =>
                                    new SplitBlockTxesStorage(splitCount, index => new LmdbBlockTxesStorage(path, this.blockTxesSize / blockTxesStorageLocations.Length / splitCount, index)));
                            }
                        }

                return this.blockTxesStorage;
            }
        }

        public DisposeHandle<IChainStateCursor> OpenChainStateCursor()
        {
            if (this.chainStateManager == null)
                lock (this.chainStateManagerLock)
                    if (this.chainStateManager == null)
                        this.chainStateManager = new LmdbChainStateManager(this.baseDirectory, this.chainStateSize);

            return this.chainStateManager.OpenChainStateCursor();
        }

        public DisposeHandle<IDeferredChainStateCursor> OpenDeferredChainStateCursor(IChainState chainState)
        {
            var cursor = new DeferredChainStateCursor(chainState, this);
            return new DisposeHandle<IDeferredChainStateCursor>(
                _ => cursor.Dispose(), cursor);
        }

        internal static void PrepareSparseDatabase(string jetDirectory)
        {
            // detect windows OS
            var isWindows = Environment.OSVersion.Platform != PlatformID.MacOSX && Environment.OSVersion.Platform != PlatformID.Unix;
            if (!isWindows)
                return;

            // ensure db is created
            using (var jetInstance = new LightningEnvironment(jetDirectory))
                jetInstance.Open();

            // check if db is on NTFS filesystem
            var dbPath = Path.Combine(jetDirectory, "data.mdb");
            var dbFileRoot = Path.GetPathRoot(dbPath);
            var dbDrive = DriveInfo.GetDrives().FirstOrDefault(x => x.RootDirectory.FullName == dbFileRoot);
            if (dbDrive != null && dbDrive.DriveFormat == "NTFS")
            {
                //TODO better way to set the sparse flag?
                // ensure db is sparse
                Process.Start(new ProcessStartInfo
                {
                    FileName = "fsutil.exe",
                    WorkingDirectory = jetDirectory,
                    Arguments = "sparse setflag data.mdb",
                    CreateNoWindow = true,
                    WindowStyle = ProcessWindowStyle.Hidden
                }).WaitForExit();
            }
        }
    }
}
