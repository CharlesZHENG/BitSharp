using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage;
using BitSharp.Core.Storage.Memory;
using NLog;
using System;

namespace BitSharp.LevelDb
{
    public class LevelDbStorageManager : IStorageManager
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private const int KiB = 1024;
        private const int MiB = KiB * KiB;

        private readonly string baseDirectory;
        private readonly string[] blockTxesStorageLocations;

        private Lazy<LevelDbBlockStorage> blockStorage;
        private Lazy<IBlockTxesStorage> blockTxesStorage;
        private Lazy<LevelDbChainStateManager> chainStateManager;
        //TODO memory storage should not be used for unconfirmed txes
        private Lazy<MemoryStorageManager> memoryStorageManager;

        private bool isDisposed;

        public LevelDbStorageManager(string baseDirectory,
            ulong? blocksCacheSize = null, ulong? blocksWriteCacheSize = null,
            ulong? blockTxesCacheSize = null, ulong? blockTxesWriteCacheSize = null,
            ulong? chainStateCacheSize = null, ulong? chainStateWriteCacheSize = null,
            string[] blockTxesStorageLocations = null)
        {
            this.baseDirectory = baseDirectory;
            this.blockTxesStorageLocations = blockTxesStorageLocations;

            blockStorage = new Lazy<LevelDb.LevelDbBlockStorage>(() => new LevelDbBlockStorage(this.baseDirectory, blocksCacheSize, blocksWriteCacheSize));

            blockTxesStorage = new Lazy<IBlockTxesStorage>(() =>
            {
                if (blockTxesStorageLocations == null)
                    return new LevelDbBlockTxesStorage(this.baseDirectory, blockTxesCacheSize, blockTxesWriteCacheSize);
                else
                    return new SplitBlockTxesStorage(blockTxesStorageLocations, path => new LevelDbBlockTxesStorage(path, blockTxesCacheSize, blockTxesWriteCacheSize));
            });

            chainStateManager = new Lazy<LevelDbChainStateManager>(() => new LevelDbChainStateManager(this.baseDirectory, chainStateCacheSize, chainStateWriteCacheSize));
            this.memoryStorageManager = new Lazy<MemoryStorageManager>(() =>
            {
                // create memory storage with the unconfirmed txes chain tip already in sync with chain state
                ChainedHeader chainTip;
                using (var handle = OpenChainStateCursor())
                {
                    handle.Item.BeginTransaction(readOnly: true);
                    chainTip = handle.Item.ChainTip;
                }

                var memoryStorageManager = new MemoryStorageManager();
                using (var handle = memoryStorageManager.OpenUnconfirmedTxesCursor())
                {
                    handle.Item.BeginTransaction();
                    handle.Item.ChainTip = chainTip;
                    handle.Item.CommitTransaction();
                }

                return memoryStorageManager;
            });
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
                if (chainStateManager.IsValueCreated)
                    chainStateManager.Value.Dispose();

                if (blockStorage.IsValueCreated)
                    blockStorage.Value.Dispose();

                if (blockTxesStorage.IsValueCreated)
                    blockTxesStorage.Value.Dispose();

                if (memoryStorageManager.IsValueCreated)
                    memoryStorageManager.Value.Dispose();

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
            return chainStateManager.Value.OpenDeferredChainStateCursor();
        }

        //TODO should be true once its not backed by memory
        public bool IsUnconfirmedTxesConcurrent { get; } = false;

        public DisposeHandle<IUnconfirmedTxesCursor> OpenUnconfirmedTxesCursor()
        {
            return memoryStorageManager.Value.OpenUnconfirmedTxesCursor();
        }
    }
}
