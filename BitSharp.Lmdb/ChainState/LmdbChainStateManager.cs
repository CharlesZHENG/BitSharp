using BitSharp.Common;
using BitSharp.Core.Storage;
using LightningDB;
using NLog;
using System;
using System.IO;

namespace BitSharp.Lmdb
{
    internal class LmdbChainStateManager : IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly string baseDirectory;
        private readonly string jetDirectory;
        private readonly string jetDatabase;
        private readonly LightningEnvironment jetInstance;
        private readonly LightningDatabase globalsTableId;
        private readonly LightningDatabase headersTableId;
        private readonly LightningDatabase unspentTxTableId;
        private readonly LightningDatabase blockSpentTxesTableId;
        private readonly LightningDatabase blockUnmintedTxesTableId;

        public LmdbChainStateManager(string baseDirectory, long chainStateSize)
        {
            this.baseDirectory = baseDirectory;
            this.jetDirectory = Path.Combine(baseDirectory, "ChainState");
            this.jetDatabase = Path.Combine(this.jetDirectory, "ChainState.edb");

            LmdbStorageManager.PrepareSparseDatabase(this.jetDirectory);
            this.jetInstance = new LightningEnvironment(this.jetDirectory, EnvironmentOpenFlags.NoThreadLocalStorage | EnvironmentOpenFlags.NoSync)
            {
                MaxDatabases = 10,
                MapSize = chainStateSize
            };
            this.jetInstance.Open();

            using (var txn = this.jetInstance.BeginTransaction())
            {
                globalsTableId = txn.OpenDatabase("Globals", new DatabaseOptions { Flags = DatabaseOpenFlags.Create });
                headersTableId = txn.OpenDatabase("Headers", new DatabaseOptions { Flags = DatabaseOpenFlags.Create });
                unspentTxTableId = txn.OpenDatabase("UnspentTx", new DatabaseOptions { Flags = DatabaseOpenFlags.Create });
                blockSpentTxesTableId = txn.OpenDatabase("BlockSpentTxes", new DatabaseOptions { Flags = DatabaseOpenFlags.Create });
                blockUnmintedTxesTableId = txn.OpenDatabase("BlockUnmintedTxes", new DatabaseOptions { Flags = DatabaseOpenFlags.Create });

                txn.Commit();
            }
        }

        public void Dispose()
        {
            this.jetInstance.Dispose();
        }

        public DisposeHandle<IChainStateCursor> OpenChainStateCursor()
        {
            var cursor = new ChainStateCursor(false, this.jetDatabase, this.jetInstance, globalsTableId, headersTableId, unspentTxTableId, blockSpentTxesTableId, blockUnmintedTxesTableId);
            return new DisposeHandle<IChainStateCursor>(() => cursor.Dispose(), cursor);
        }
    }
}
