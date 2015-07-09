using BitSharp.Common;
using BitSharp.Core.Storage;
using BitSharp.Esent.ChainState;
using Microsoft.Isam.Esent.Interop;
using Microsoft.Isam.Esent.Interop.Windows81;
using NLog;
using System;
using System.IO;

namespace BitSharp.Esent
{
    internal class EsentChainStateManager : IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly string baseDirectory;
        private readonly string jetDirectory;
        private readonly string jetDatabase;
        private readonly Instance jetInstance;

        private readonly DisposableCache<IChainStateCursor> cursorCache;

        public EsentChainStateManager(string baseDirectory)
        {
            this.baseDirectory = baseDirectory;
            this.jetDirectory = Path.Combine(baseDirectory, "ChainState");
            this.jetDatabase = Path.Combine(this.jetDirectory, "ChainState.edb");

            this.jetInstance = CreateInstance(this.jetDirectory);
            this.jetInstance.Init();

            this.CreateOrOpenDatabase();

            this.cursorCache = new DisposableCache<IChainStateCursor>(256,
                createFunc: () => new ChainStateCursor(this.jetDatabase, this.jetInstance),
                prepareAction: cursor =>
                {
                    // rollback any open transaction before returning the cursor to the cache
                    if (cursor.InTransaction)
                        cursor.RollbackTransaction();
                });
        }

        public void Dispose()
        {
            this.cursorCache.Dispose();
            this.jetInstance.Dispose();
        }

        public DisposeHandle<IChainStateCursor> OpenChainStateCursor()
        {
            return this.cursorCache.TakeItem();
        }

        private void CreateOrOpenDatabase()
        {
            try
            {
                ChainStateSchema.OpenDatabase(this.jetDatabase, this.jetInstance, readOnly: false);
            }
            catch (Exception)
            {
                try { Directory.Delete(this.jetDirectory, recursive: true); }
                catch (Exception) { }
                Directory.CreateDirectory(this.jetDirectory);

                ChainStateSchema.CreateDatabase(this.jetDatabase, this.jetInstance);
            }
        }

        private static Instance CreateInstance(string directory)
        {
            var instance = new Instance(Guid.NewGuid().ToString());

            instance.Parameters.SystemDirectory = directory;
            instance.Parameters.LogFileDirectory = directory;
            instance.Parameters.TempDirectory = directory;
            instance.Parameters.AlternateDatabaseRecoveryDirectory = directory;
            instance.Parameters.CreatePathIfNotExist = true;
            instance.Parameters.BaseName = "epc";
            instance.Parameters.EnableIndexChecking = false;
            instance.Parameters.CircularLog = true;
            instance.Parameters.CheckpointDepthMax = 64 * 1024 * 1024;
            instance.Parameters.LogFileSize = 1024;
            instance.Parameters.LogBuffers = 1024;
            instance.Parameters.CleanupMismatchedLogFiles = true;
            instance.Parameters.MaxTemporaryTables = 16;
            instance.Parameters.MaxVerPages = 1024 * 256;
            instance.Parameters.NoInformationEvent = true;
            instance.Parameters.WaypointLatency = 1;
            instance.Parameters.MaxSessions = 256;
            instance.Parameters.MaxCursors = 256 * 10;
            instance.Parameters.MaxOpenTables = 256 * 10;
            if (EsentVersion.SupportsWindows81Features)
            {
                instance.Parameters.EnableShrinkDatabase = ShrinkDatabaseGrbit.On | ShrinkDatabaseGrbit.Realtime;
            }

            return instance;
        }
    }
}
