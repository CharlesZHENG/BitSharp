using BitSharp.Common;
using BitSharp.Core.Builders;
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
        private readonly DisposableCache<IDeferredChainStateCursor> deferredCursorCache;

        public LevelDbChainStateManager(string baseDirectory, ulong? cacheSize, ulong? writeCacheSize)
        {
            this.baseDirectory = baseDirectory;
            dbDirectory = Path.Combine(baseDirectory, "ChainState");
            dbFile = Path.Combine(dbDirectory, "ChainState.edb");

            db = DB.Open(dbFile, cacheSize ?? 0, writeCacheSize ?? 0);

            cursorCache = new DisposableCache<IChainStateCursor>(1024,
                createFunc: () => new LevelDbChainStateCursor(dbFile, db, isDeferred: false),
                prepareAction: cursor =>
                {
                    // rollback any open transaction before returning the cursor to the cache
                    if (cursor.InTransaction)
                        cursor.RollbackTransaction();
                });

            deferredCursorCache = new DisposableCache<IDeferredChainStateCursor>(1,
                createFunc: () => new LevelDbChainStateCursor(dbFile, db, isDeferred: true),
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
            deferredCursorCache.Dispose();
            db.Dispose();
        }

        public DisposeHandle<IChainStateCursor> OpenChainStateCursor()
        {
            return cursorCache.TakeItem();
        }

        public DisposeHandle<IDeferredChainStateCursor> OpenDeferredChainStateCursor()
        {
            return deferredCursorCache.TakeItem();
        }
    }
}
