using BitSharp.Common;
using BitSharp.Core.Storage;
using LevelDB;
using NLog;
using System;
using System.IO;

namespace BitSharp.LevelDb
{
    internal class LevelDbChainStateManager : IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly string baseDirectory;
        private readonly string dbDirectory;
        private readonly string dbFile;
        private readonly DB db;

        private readonly DisposableCache<IChainStateCursor> cursorCache;

        public LevelDbChainStateManager(string baseDirectory)
        {
            this.baseDirectory = baseDirectory;
            dbDirectory = Path.Combine(baseDirectory, "ChainState");
            dbFile = Path.Combine(dbDirectory, "ChainState.edb");

            db = DB.Open(dbFile);

            cursorCache = new DisposableCache<IChainStateCursor>(1024,
                createFunc: () => new LevelDbChainStateCursor(dbFile, db),
                prepareAction: cursor =>
                {
                    // rollback any open transaction before returning the cursor to the cache
                    if (cursor.InTransaction)
                        cursor.RollbackTransaction();
                });
        }

        public void Dispose()
        {
            cursorCache.Dispose();
            db.Dispose();
        }

        public DisposeHandle<IChainStateCursor> OpenChainStateCursor()
        {
            return cursorCache.TakeItem();
        }
    }
}
