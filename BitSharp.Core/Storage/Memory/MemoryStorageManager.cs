using BitSharp.Common;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using System;
using System.Collections.Immutable;

namespace BitSharp.Core.Storage.Memory
{
    public class MemoryStorageManager : IStorageManager
    {
        private readonly MemoryBlockStorage blockStorage;
        private readonly MemoryBlockTxesStorage blockTxesStorage;
        private readonly MemoryChainStateStorage chainStateStorage;
        private readonly MemoryUnconfirmedTxesStorage unconfirmedTxesStorage;

        private readonly DisposableCache<IChainStateCursor> chainStateCursorCache;
        private readonly DisposableCache<IUnconfirmedTxesCursor> unconfirmedTxesCursorCache;

        private bool isDisposed;

        public MemoryStorageManager()
            : this(null, null, null, null)
        { }

        internal MemoryStorageManager(ChainedHeader chainTip = null, int? unspentTxCount = null, int? unspentOutputCount = null, int? totalTxCount = null, int? totalInputCount = null, int? totalOutputCount = null, ImmutableSortedDictionary<UInt256, ChainedHeader> headers = null, ImmutableSortedDictionary<UInt256, UnspentTx> unspentTransactions = null, ImmutableDictionary<int, BlockSpentTxes> spentTransactions = null)
        {
            blockStorage = new MemoryBlockStorage();
            blockTxesStorage = new MemoryBlockTxesStorage();
            chainStateStorage = new MemoryChainStateStorage(chainTip, unspentTxCount, unspentOutputCount, totalTxCount, totalInputCount, totalOutputCount, headers, unspentTransactions, spentTransactions);
            unconfirmedTxesStorage = new MemoryUnconfirmedTxesStorage();

            chainStateCursorCache = new DisposableCache<IChainStateCursor>(1024,
                createFunc: () => new MemoryChainStateCursor(chainStateStorage),
                prepareAction: cursor =>
                {
                    // rollback any open transaction before returning the cursor to the cache
                    if (cursor.InTransaction)
                        cursor.RollbackTransaction();
                });

            unconfirmedTxesCursorCache = new DisposableCache<IUnconfirmedTxesCursor>(1024,
                createFunc: () => new MemoryUnconfirmedTxesCursor(unconfirmedTxesStorage),
                prepareAction: cursor =>
                {
                    // rollback any open transaction before returning the cursor to the cache
                    if (cursor.InTransaction)
                        cursor.RollbackTransaction();
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
                chainStateCursorCache.Dispose();
                blockStorage.Dispose();
                blockTxesStorage.Dispose();
                chainStateStorage.Dispose();

                isDisposed = true;
            }
        }

        public IBlockStorage BlockStorage => blockStorage;

        public IBlockTxesStorage BlockTxesStorage => blockTxesStorage;

        public DisposeHandle<IChainStateCursor> OpenChainStateCursor()
        {
            return chainStateCursorCache.TakeItem();
        }

        public DisposeHandle<IDeferredChainStateCursor> OpenDeferredChainStateCursor(IChainState chainState)
        {
            var cursor = new DeferredChainStateCursor(chainState, this);
            return new DisposeHandle<IDeferredChainStateCursor>(
                _ => cursor.Dispose(), cursor);
        }

        public bool IsUnconfirmedTxesConcurrent { get; } = false;

        public DisposeHandle<IUnconfirmedTxesCursor> OpenUnconfirmedTxesCursor()
        {
            return unconfirmedTxesCursorCache.TakeItem();
        }
    }
}
