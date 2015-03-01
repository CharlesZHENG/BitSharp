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
        private readonly Chain chain;

        private readonly DisposableCache<DisposeHandle<IChainStateCursor>> cursorCache;

        public ChainState(Chain chain, IStorageManager storageManager)
        {
            this.chain = chain;

            // create a cache of cursors that are in an open snapshot transaction with the current chain state
            this.cursorCache = new DisposableCache<DisposeHandle<IChainStateCursor>>(16);
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
                    cursor.BeginTransaction();

                    // verify the chain state matches the expected chain
                    var chainTip = cursor.GetChainTip();
                    if (chainTip != chain.LastBlock)
                        throw new InvalidOperationException();
                }
            }
            catch (Exception)
            {
                // ensure any opened cursors are cleaned up on an error
                this.cursorCache.Dispose();
                throw;
            }
        }

        ~ChainState()
        {
            this.Dispose();
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);

            this.cursorCache.Dispose();
        }

        public Chain Chain
        {
            get { return this.chain; }
        }

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

        public bool TryGetBlockSpentTxes(int blockIndex, out IImmutableList<SpentTx> spentTxes)
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
