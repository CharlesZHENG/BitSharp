﻿using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
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
        private readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly string baseDirectory;
        private readonly long blocksSize;
        private readonly long blockTxesSize;
        private readonly long chainStateSize;

        private readonly object blockStorageLock;
        private readonly object blockTxesStorageLock;
        private readonly object chainStateManagerLock;

        private BlockStorage blockStorage;
        private BlockTxesStorage blockTxesStorage;
        private LmdbChainStateManager chainStateManager;

        public LmdbStorageManager(string baseDirectory)
            : this(baseDirectory, blocksSize: 4.BILLION(), blockTxesSize: 128.BILLION(), chainStateSize: 32.BILLION())
        {
        }

        public LmdbStorageManager(string baseDirectory, long blocksSize, long blockTxesSize, long chainStateSize)
        {
            this.baseDirectory = baseDirectory;
            this.blocksSize = blocksSize;
            this.blockTxesSize = blockTxesSize;
            this.chainStateSize = chainStateSize;

            this.blockStorageLock = new object();
            this.blockTxesStorageLock = new object();
            this.chainStateManagerLock = new object();
        }

        public void Dispose()
        {
            if (this.chainStateManager != null)
                this.chainStateManager.Dispose();

            if (this.blockStorage != null)
                this.blockStorage.Dispose();

            if (this.blockTxesStorage != null)
                this.blockTxesStorage.Dispose();
        }

        public IBlockStorage BlockStorage
        {
            get
            {
                if (this.blockStorage == null)
                    lock (this.blockStorageLock)
                        if (this.blockStorage == null)
                            this.blockStorage = new BlockStorage(this.baseDirectory, this.blocksSize);

                return this.blockStorage;
            }
        }

        public IBlockTxesStorage BlockTxesStorage
        {
            get
            {
                if (this.blockTxesStorage == null)
                    lock (this.blockTxesStorageLock)
                        if (this.blockTxesStorage == null)
                            this.blockTxesStorage = new BlockTxesStorage(this.baseDirectory, this.blockTxesSize);

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