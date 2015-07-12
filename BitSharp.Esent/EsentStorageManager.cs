using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Storage;
using Microsoft.Isam.Esent.Collections.Generic;
using Microsoft.Isam.Esent.Interop;
using Microsoft.Isam.Esent.Interop.Windows81;
using NLog;
using System;

namespace BitSharp.Esent
{
    public class EsentStorageManager : IStorageManager
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private const int KiB = 1024;
        private const int MiB = KiB * KiB;

        private readonly string baseDirectory;
        private readonly string[] blockTxesStorageLocations;

        private readonly object blockStorageLock;
        private readonly object blockTxesStorageLock;
        private readonly object chainStateManagerLock;

        private BlockStorage blockStorage;
        private IBlockTxesStorage blockTxesStorage;
        private EsentChainStateManager chainStateManager;

        private bool isDisposed;

        public EsentStorageManager(string baseDirectory, string[] blockTxesStorageLocations = null)
        {
            this.baseDirectory = baseDirectory;
            this.blockTxesStorageLocations = blockTxesStorageLocations;

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
                            this.blockStorage = new BlockStorage(this.baseDirectory);

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
                        {
                            if (blockTxesStorageLocations == null)
                                this.blockTxesStorage = new BlockTxesStorage(this.baseDirectory);
                            else
                                this.blockTxesStorage = new SplitBlockTxesStorage(blockTxesStorageLocations, path => new BlockTxesStorage(path));
                        }

                return this.blockTxesStorage;
            }
        }

        public DisposeHandle<IChainStateCursor> OpenChainStateCursor()
        {
            if (this.chainStateManager == null)
                lock (this.chainStateManagerLock)
                    if (this.chainStateManager == null)
                        this.chainStateManager = new EsentChainStateManager(this.baseDirectory);

            return this.chainStateManager.OpenChainStateCursor();
        }

        internal static void InitSystemParameters(long? cacheSizeMinBytes = null, long? cacheSizeMaxBytes = null)
        {
            //TODO remove reflection once PersistentDictionary is phased out
            var esentAssembly = typeof(PersistentDictionary<string, string>).Assembly;
            var type = esentAssembly.GetType("Microsoft.Isam.Esent.Collections.Generic.CollectionsSystemParameters");
            var method = type.GetMethod("Init");
            method.Invoke(null, null);

            SystemParameters.DatabasePageSize = 8 * KiB;

            if (cacheSizeMinBytes != null)
                SystemParameters.CacheSizeMin = (cacheSizeMinBytes.Value / SystemParameters.DatabasePageSize).ToIntChecked();
            if (cacheSizeMaxBytes != null)
                SystemParameters.CacheSizeMax = (cacheSizeMaxBytes.Value / SystemParameters.DatabasePageSize).ToIntChecked();
        }

        internal static void InitInstanceParameters(Instance instance, string directory)
        {
            var _0_5KiB = KiB / 2;
            var _16KiB = 16 * KiB;
            
            var _16MiB = 16 * MiB;
            var _32MiB = 32 * MiB;
            var _256MiB = 256 * MiB;
            
            var logFileCount = 32;

            instance.Parameters.SystemDirectory = directory;
            instance.Parameters.LogFileDirectory = directory;
            instance.Parameters.TempDirectory = directory;
            instance.Parameters.AlternateDatabaseRecoveryDirectory = directory;
            instance.Parameters.CreatePathIfNotExist = true;
            instance.Parameters.BaseName = "epc";
            instance.Parameters.EnableOnlineDefrag = true;
            instance.Parameters.EnableIndexChecking = false;
            instance.Parameters.NoInformationEvent = true;
            instance.Parameters.MaxSessions = 30000;
            instance.Parameters.MaxCursors = int.MaxValue;
            instance.Parameters.MaxOpenTables = int.MaxValue;
            instance.Parameters.MaxTemporaryTables = 16;
            instance.Parameters.CircularLog = true;
            instance.Parameters.CleanupMismatchedLogFiles = true;
            
            // unit is KiB
            instance.Parameters.LogFileSize = _32MiB / KiB;
            // unit is 0.5KiB
            instance.Parameters.LogBuffers = _16MiB / _0_5KiB;
            // unit is bytes
            instance.Parameters.CheckpointDepthMax = logFileCount * (instance.Parameters.LogFileSize * KiB);
            // unit is 16KiB
            instance.Parameters.MaxVerPages = _256MiB / _16KiB;
            
            if (EsentVersion.SupportsWindows81Features)
                instance.Parameters.EnableShrinkDatabase = ShrinkDatabaseGrbit.On | ShrinkDatabaseGrbit.Realtime;
        }
    }
}
