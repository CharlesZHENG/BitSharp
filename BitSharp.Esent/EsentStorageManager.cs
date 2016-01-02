using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
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

        private Lazy<EsentBlockStorage> blockStorage;
        private Lazy<IBlockTxesStorage> blockTxesStorage;
        private Lazy<EsentChainStateManager> chainStateManager;

        private bool isDisposed;

        public EsentStorageManager(string baseDirectory, string[] blockTxesStorageLocations = null)
        {
            this.baseDirectory = baseDirectory;
            this.blockTxesStorageLocations = blockTxesStorageLocations;

            this.blockStorage = new Lazy<Esent.EsentBlockStorage>(() => new EsentBlockStorage(this.baseDirectory));

            this.blockTxesStorage = new Lazy<IBlockTxesStorage>(() =>
            {
                if (blockTxesStorageLocations == null)
                    return new EsentBlockTxesStorage(this.baseDirectory);
                else
                    return new SplitBlockTxesStorage(blockTxesStorageLocations, path => new EsentBlockTxesStorage(path));
            });

            this.chainStateManager = new Lazy<EsentChainStateManager>(() => new EsentChainStateManager(this.baseDirectory));
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
                if (this.chainStateManager.IsValueCreated)
                    this.chainStateManager.Value.Dispose();

                if (this.blockStorage.IsValueCreated)
                    this.blockStorage.Value.Dispose();

                if (this.blockTxesStorage.IsValueCreated)
                    this.blockTxesStorage.Value.Dispose();

                isDisposed = true;
            }
        }

        public IBlockStorage BlockStorage => blockStorage.Value;

        public IBlockTxesStorage BlockTxesStorage => blockTxesStorage.Value;

        public DisposeHandle<IChainStateCursor> OpenChainStateCursor()
        {
            return chainStateManager.Value.OpenChainStateCursor();
        }

        public DisposeHandle<IDeferredChainStateCursor> OpenDeferredChainStateCursor(IChainState chainState)
        {
            var cursor = new DeferredChainStateCursor(chainState, this);
            return new DisposeHandle<IDeferredChainStateCursor>(
                _ => cursor.Dispose(), cursor);
        }

        public DisposeHandle<IUnconfirmedTxesCursor> OpenUnconfirmedTxesCursor()
        {
            throw new NotImplementedException();
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
