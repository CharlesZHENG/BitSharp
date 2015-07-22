using BitSharp.Common;
using BitSharp.Core.Storage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Core.Domain
{
    internal class ChainState : IChainState
    {
        private readonly DisposableCache<DisposeHandle<IChainStateCursor>> cursorCache;

        private bool disposed;

        public ChainState(Chain chain, IStorageManager storageManager)
        {
            CursorCount = 32;
            Chain = chain;

            // create a cache of cursors that are in an open snapshot transaction with the current chain state
            var success = false;
            this.cursorCache = new DisposableCache<DisposeHandle<IChainStateCursor>>(CursorCount);
            try
            {
                for (var i = 0; i < this.cursorCache.Capacity; i++)
                {
                    // open the cursor
                    var handle = storageManager.OpenChainStateCursor();
                    var cursor = handle.Item;

                    // cache the cursor
                    // this must be done before beginning the transaction as caching will rollback any transactions
                    this.cursorCache.CacheItem(handle);

                    // begin transaction to take the snapshot
                    cursor.BeginTransaction(readOnly: true);

                    // verify the chain state matches the expected chain
                    var chainTip = cursor.ChainTip;
                    if (chainTip != chain.LastBlock)
                        throw new ChainStateOutOfSyncException(chain.LastBlock, chainTip);
                }

                success = true;
            }
            finally
            {
                // ensure any opened cursors are cleaned up on an error
                if (!success)
                    this.cursorCache.Dispose();
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposed && disposing)
            {
                this.cursorCache.Dispose();

                disposed = true;
            }
        }

        public int CursorCount { get; }

        public Chain Chain { get; }

        public int UnspentTxCount
        {
            get
            {
                using (var handle = this.cursorCache.TakeItem())
                {
                    var cursor = handle.Item.Item;
                    return cursor.UnspentTxCount;
                }
            }
        }

        public int UnspentOutputCount
        {
            get
            {
                using (var handle = this.cursorCache.TakeItem())
                {
                    var cursor = handle.Item.Item;
                    return cursor.UnspentOutputCount;
                }
            }
        }

        public int TotalTxCount
        {
            get
            {
                using (var handle = this.cursorCache.TakeItem())
                {
                    var cursor = handle.Item.Item;
                    return cursor.TotalTxCount;
                }
            }
        }

        public int TotalInputCount
        {
            get
            {
                using (var handle = this.cursorCache.TakeItem())
                {
                    var cursor = handle.Item.Item;
                    return cursor.TotalInputCount;
                }
            }
        }

        public int TotalOutputCount
        {
            get
            {
                using (var handle = this.cursorCache.TakeItem())
                {
                    var cursor = handle.Item.Item;
                    return cursor.TotalOutputCount;
                }
            }
        }

        public bool ContainsHeader(UInt256 blockHash)
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item.Item;

                ChainedHeader header;
                return cursor.TryGetHeader(blockHash, out header);
            }
        }

        public bool TryGetHeader(UInt256 blockHash, out ChainedHeader header)
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item.Item;
                return cursor.TryGetHeader(blockHash, out header);
            }
        }

        public bool ContainsUnspentTx(UInt256 txHash)
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item.Item;

                UnspentTx unspentTx;
                return cursor.TryGetUnspentTx(txHash, out unspentTx)
                    && !unspentTx.IsFullySpent;
            }
        }

        public bool TryGetUnspentTx(UInt256 txHash, out UnspentTx unspentTx)
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item.Item;
                return cursor.TryGetUnspentTx(txHash, out unspentTx);
            }
        }

        public IEnumerable<UnspentTx> ReadUnspentTransactions()
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item.Item;
                foreach (var unspentTx in cursor.ReadUnspentTransactions().Where(x => !x.IsFullySpent))
                {
                    yield return unspentTx;
                }
            }
        }

        public bool ContainsBlockSpentTxes(int blockIndex)
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item.Item;
                return cursor.ContainsBlockSpentTxes(blockIndex);
            }
        }

        public bool TryGetBlockSpentTxes(int blockIndex, out BlockSpentTxes spentTxes)
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item.Item;
                return cursor.TryGetBlockSpentTxes(blockIndex, out spentTxes);
            }
        }

        public bool ContainsBlockUnmintedTxes(UInt256 blockHash)
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item.Item;
                return cursor.ContainsBlockUnmintedTxes(blockHash);
            }
        }

        public bool TryGetBlockUnmintedTxes(UInt256 blockHash, out IImmutableList<UnmintedTx> unmintedTxes)
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item.Item;
                return cursor.TryGetBlockUnmintedTxes(blockHash, out unmintedTxes);
            }
        }
    }
}
