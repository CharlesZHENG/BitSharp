using BitSharp.Common;
using BitSharp.Core.Storage;
using BitSharp.Esent.ChainState;
using Microsoft.Isam.Esent.Interop;
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

            this.cursorCache = new DisposableCache<IChainStateCursor>(1024,
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
            EsentStorageManager.InitInstanceParameters(instance, directory);
            return instance;
        }
    }
}
