using BitSharp.Common;
using BitSharp.Core.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Immutable;

namespace BitSharp.Core.Domain
{
    internal class UnconfirmedTxes : IUnconfirmedTxes
    {
        private readonly DisposableCache<DisposeHandle<IUnconfirmedTxesCursor>> cursorCache;

        private bool disposed;

        public UnconfirmedTxes(Chain chain, IStorageManager storageManager)
        {
            CursorCount = 32;
            Chain = chain;

            // create a cache of cursors that are in an open snapshot transaction with the current chain state
            var success = false;
            this.cursorCache = new DisposableCache<DisposeHandle<IUnconfirmedTxesCursor>>(CursorCount);
            try
            {
                for (var i = 0; i < this.cursorCache.Capacity; i++)
                {
                    // open the cursor
                    var handle = storageManager.OpenUnconfirmedTxesCursor();
                    var cursor = handle.Item;

                    // cache the cursor
                    // this must be done before beginning the transaction as caching will rollback any transactions
                    this.cursorCache.CacheItem(handle);

                    // begin transaction to take the snapshot
                    cursor.BeginTransaction(readOnly: true);

                    // verify the chain state matches the expected chain
                    var chainTip = cursor.ChainTip;
                    if (chainTip?.Hash != chain.LastBlock?.Hash)
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

        public bool ContainsTransaction(UInt256 txHash)
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item.Item;

                UnconfirmedTx unconfirmedTx;
                return cursor.TryGetTransaction(txHash, out unconfirmedTx);
            }
        }

        public bool TryGetTransaction(UInt256 txHash, out UnconfirmedTx unconfirmedTx)
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item.Item;
                return cursor.TryGetTransaction(txHash, out unconfirmedTx);
            }
        }

        public ImmutableDictionary<UInt256, UnconfirmedTx> GetTransactionsSpending(TxOutputKey txOutputKey)
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item.Item;
                return cursor.GetTransactionsSpending(txOutputKey);
            }
        }
    }
}
