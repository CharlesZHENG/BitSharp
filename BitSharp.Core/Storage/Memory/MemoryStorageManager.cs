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

        private readonly DisposableCache<IChainStateCursor> cursorCache;

        private bool isDisposed;

        public MemoryStorageManager()
            : this(null, null, null, null)
        { }

        internal MemoryStorageManager(ChainedHeader chainTip = null, int? unspentTxCount = null, int? unspentOutputCount = null, int? totalTxCount = null, int? totalInputCount = null, int? totalOutputCount = null, ImmutableSortedDictionary<UInt256, ChainedHeader> headers = null, ImmutableSortedDictionary<UInt256, UnspentTx> unspentTransactions = null, ImmutableDictionary<int, BlockSpentTxes> spentTransactions = null)
        {
            this.blockStorage = new MemoryBlockStorage();
            this.blockTxesStorage = new MemoryBlockTxesStorage();
            this.chainStateStorage = new MemoryChainStateStorage(chainTip, unspentTxCount, unspentOutputCount, totalTxCount, totalInputCount, totalOutputCount, headers, unspentTransactions, spentTransactions);

            this.cursorCache = new DisposableCache<IChainStateCursor>(1024,
                createFunc: () => new MemoryChainStateCursor(this.chainStateStorage),
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
                this.cursorCache.Dispose();
                this.blockStorage.Dispose();
                this.blockTxesStorage.Dispose();
                this.chainStateStorage.Dispose();

                isDisposed = true;
            }
        }

        public IBlockStorage BlockStorage
        {
            get { return this.blockStorage; }
        }

        public IBlockTxesStorage BlockTxesStorage
        {
            get { return this.blockTxesStorage; }
        }

        public DisposeHandle<IChainStateCursor> OpenChainStateCursor()
        {
            return this.cursorCache.TakeItem();
        }

        public DisposeHandle<IDeferredChainStateCursor> OpenDeferredChainStateCursor(IChainState chainState)
        {
            var cursor = new DeferredChainStateCursor(chainState, this);
            return new DisposeHandle<IDeferredChainStateCursor>(
                () => cursor.Dispose(), cursor);
        }
    }
}
