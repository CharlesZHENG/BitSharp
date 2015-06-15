using BitSharp.Common;
using BitSharp.Core.Domain;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.Caching;
using System.Threading;

namespace BitSharp.Core.Storage
{
    public class CoreStorage : IDisposable
    {
        private readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly IStorageManager storageManager;
        private readonly IBlockStorage blockStorage;
        private readonly IBlockTxesStorage blockTxesStorage;

        private readonly ConcurrentDictionary<UInt256, ChainedHeader> cachedHeaders;

        private readonly ConcurrentDictionary<UInt256, bool> presentBlockTxes = new ConcurrentDictionary<UInt256, bool>();
        private readonly object[] presentBlockTxesLocks = new object[64];

        private readonly MemoryCache txCache = new MemoryCache("CoreStorage.TxCache");

        public CoreStorage(IStorageManager storageManager)
        {
            for (var i = 0; i < this.presentBlockTxesLocks.Length; i++)
                presentBlockTxesLocks[i] = new object();

            this.storageManager = storageManager;
            this.blockStorage = storageManager.BlockStorage;
            this.blockTxesStorage = storageManager.BlockTxesStorage;

            this.cachedHeaders = new ConcurrentDictionary<UInt256, ChainedHeader>();
            foreach (var chainedHeader in this.blockStorage.ReadChainedHeaders())
                this.cachedHeaders[chainedHeader.Hash] = chainedHeader;
        }

        public void Dispose()
        {
            this.txCache.Dispose();
        }

        public event Action<ChainedHeader> ChainedHeaderAdded;

        public event Action<ChainedHeader> BlockTxesAdded;

        public event Action<ChainedHeader> BlockTxesRemoved;

        public event Action<UInt256> BlockInvalidated;

        public int ChainedHeaderCount { get { return -1; } }

        public int BlockWithTxesCount { get { return this.blockTxesStorage.BlockCount; } }

        internal IStorageManager StorageManager { get { return this.storageManager; } }

        public bool ContainsChainedHeader(UInt256 blockHash)
        {
            ChainedHeader chainedHeader;
            return TryGetChainedHeader(blockHash, out chainedHeader);
        }

        public void AddGenesisBlock(ChainedHeader genesisHeader)
        {
            if (genesisHeader.Height != 0)
                throw new ArgumentException("genesisHeader");

            if (this.blockStorage.TryAddChainedHeader(genesisHeader))
            {
                this.cachedHeaders[genesisHeader.Hash] = genesisHeader;
                RaiseChainedHeaderAdded(genesisHeader);
            }
        }

        public bool TryGetChainedHeader(UInt256 blockHash, out ChainedHeader chainedHeader)
        {
            if (this.cachedHeaders.TryGetValue(blockHash, out chainedHeader))
            {
                return chainedHeader != null;
            }
            else if (this.blockStorage.TryGetChainedHeader(blockHash, out chainedHeader))
            {
                this.cachedHeaders[blockHash] = chainedHeader;
                return true;
            }
            else
            {
                this.cachedHeaders[blockHash] = null;
                return false;
            }
        }

        public ChainedHeader GetChainedHeader(UInt256 blockHash)
        {
            ChainedHeader chainedHeader;
            if (TryGetChainedHeader(blockHash, out chainedHeader))
                return chainedHeader;
            else
                throw new MissingDataException(blockHash);
        }

        public void ChainHeaders(IEnumerable<BlockHeader> blockHeaders)
        {
            var added = false;
            try
            {
                foreach (var blockHeader in blockHeaders)
                {
                    ChainedHeader ignore;
                    added |= TryChainHeader(blockHeader, out ignore, suppressEvent: true);
                }
            }
            finally
            {
                if (added)
                    RaiseChainedHeaderAdded(/*TODO*/null);
            }
        }

        public bool TryChainHeader(BlockHeader blockHeader, out ChainedHeader chainedHeader)
        {
            return TryChainHeader(blockHeader, out chainedHeader, suppressEvent: false);
        }

        private bool TryChainHeader(BlockHeader blockHeader, out ChainedHeader chainedHeader, bool suppressEvent)
        {
            if (TryGetChainedHeader(blockHeader.Hash, out chainedHeader))
            {
                return false;
            }
            else
            {
                ChainedHeader previousChainedHeader;
                if (TryGetChainedHeader(blockHeader.PreviousBlock, out previousChainedHeader))
                {
                    var headerWork = blockHeader.CalculateWork();
                    if (headerWork < 0)
                        return false;

                    chainedHeader = new ChainedHeader(blockHeader,
                        previousChainedHeader.Height + 1,
                        previousChainedHeader.TotalWork + headerWork);

                    if (this.blockStorage.TryAddChainedHeader(chainedHeader))
                    {
                        this.cachedHeaders[chainedHeader.Hash] = chainedHeader;

                        if (!suppressEvent)
                            RaiseChainedHeaderAdded(chainedHeader);

                        return true;
                    }
                    else
                    {
                        this.logger.Warn("Unexpected condition: validly chained header could not be added");
                    }
                }
            }

            chainedHeader = default(ChainedHeader);
            return false;
        }

        public ChainedHeader FindMaxTotalWork()
        {
            return this.blockStorage.FindMaxTotalWork();
        }

        public bool ContainsBlockTxes(UInt256 blockHash)
        {
            lock (GetBlockLock(blockHash))
            {
                bool present;
                if (this.presentBlockTxes.TryGetValue(blockHash, out present))
                {
                    return present;
                }
                else
                {
                    present = this.blockTxesStorage.ContainsBlock(blockHash);
                    this.presentBlockTxes.TryAdd(blockHash, present);
                    return present;
                }
            };
        }

        public bool TryAddBlock(Block block)
        {
            if (this.ContainsBlockTxes(block.Hash))
                return false;

            lock (GetBlockLock(block.Hash))
            {
                ChainedHeader chainedHeader;
                if (TryGetChainedHeader(block.Hash, out chainedHeader) || TryChainHeader(block.Header, out chainedHeader))
                {
                    if (this.blockTxesStorage.TryAddBlockTransactions(block.Hash, block.Transactions))
                    {
                        this.presentBlockTxes[block.Hash] = true;
                        RaiseBlockTxesAdded(chainedHeader);
                        return true;
                    }
                }

                return false;
            };
        }

        public bool TryGetBlock(UInt256 blockHash, out Block block)
        {
            ChainedHeader chainedHeader;
            if (!TryGetChainedHeader(blockHash, out chainedHeader))
            {
                block = default(Block);
                return false;
            }

            IEnumerable<BlockTx> blockTxes;
            if (TryReadBlockTransactions(chainedHeader.Hash, chainedHeader.MerkleRoot, /*requireTransactions:*/true, out blockTxes))
            {
                var transactions = ImmutableArray.CreateRange(blockTxes.Select(x => x.Transaction));
                block = new Block(chainedHeader.BlockHeader, transactions);
                return true;
            }
            else
            {
                block = default(Block);
                return false;
            }
        }

        public bool TryGetTransaction(UInt256 blockHash, int txIndex, out Transaction transaction)
        {
            var key = blockHash.ToString() + txIndex;

            // create the lazy that will be used to load the transaction if it is not yet cached
            var lazyLoadTx = new Lazy<Transaction>(
                () =>
                {
                    Transaction tx;
                    if (this.blockTxesStorage.TryGetTransaction(blockHash, txIndex, out tx))
                        return tx;
                    else
                        return null;
                }, LazyThreadSafetyMode.ExecutionAndPublication);

            var cachedLazyTx = (Lazy<Transaction>)txCache.AddOrGetExisting(key, lazyLoadTx,
                new CacheItemPolicy { AbsoluteExpiration = DateTimeOffset.UtcNow.AddMinutes(1) });

            // get the cached transaction, or load if added to the cache
            //      note: if cachedLazyTx is null, lazyLoadTx must have been added to the cache
            transaction = (cachedLazyTx ?? lazyLoadTx).Value;

            //TODO hacky, isn't synchronized with other threads
            // ensures null isn't cached
            if (transaction == null)
                txCache.Remove(key);

            return transaction != null;
        }

        public bool TryReadBlockTransactions(UInt256 blockHash, UInt256 merkleRoot, bool requireTransactions, out IEnumerable<BlockTx> blockTxes)
        {
            IEnumerable<BlockTx> rawBlockTxes;
            if (this.blockTxesStorage.TryReadBlockTransactions(blockHash, out rawBlockTxes))
            {
                blockTxes = ReadBlockTransactions(blockHash, merkleRoot, requireTransactions, rawBlockTxes);
                return true;
            }
            else
            {
                blockTxes = null;
                return false;
            }
        }

        private IEnumerable<BlockTx> ReadBlockTransactions(UInt256 blockHash, UInt256 merkleRoot, bool requireTransactions, IEnumerable<BlockTx> blockTxes)
        {
            using (var blockTxesEnumerator = MerkleTree.ReadMerkleTreeNodes(merkleRoot, blockTxes).GetEnumerator())
            {
                while (true)
                {
                    bool read;
                    try
                    {
                        read = blockTxesEnumerator.MoveNext();
                    }
                    catch (MissingDataException e)
                    {
                        var missingBlockHash = (UInt256)e.Key;

                        lock (GetBlockLock(blockHash))
                            this.presentBlockTxes[missingBlockHash] = false;

                        throw;
                    }

                    if (read)
                    {
                        var blockTx = blockTxesEnumerator.Current;
                        if (requireTransactions && blockTx.Pruned)
                        {
                            //TODO distinguish different kinds of missing: pruned and missing entirely
                            throw new MissingDataException(blockHash);
                        }

                        yield return blockTx;
                    }
                    else
                    {
                        yield break;
                    }
                }
            }
        }

        public bool IsBlockInvalid(UInt256 blockHash)
        {
            return this.blockStorage.IsBlockInvalid(blockHash);
        }

        //TODO this should mark any blocks chained on top as invalid
        public void MarkBlockInvalid(UInt256 blockHash)
        {
            this.blockStorage.MarkBlockInvalid(blockHash);
            RaiseBlockInvalidated(blockHash);
        }

        private void RaiseChainedHeaderAdded(ChainedHeader chainedHeader)
        {
            var handler = this.ChainedHeaderAdded;
            if (handler != null)
                handler(chainedHeader);
        }

        private void RaiseBlockTxesAdded(ChainedHeader chainedHeader)
        {
            var handler = this.BlockTxesAdded;
            if (handler != null)
                handler(chainedHeader);
        }

        private void RaiseBlockTxesRemoved(ChainedHeader chainedHeader)
        {
            var handler = this.BlockTxesRemoved;
            if (handler != null)
                handler(chainedHeader);
        }

        private void RaiseBlockInvalidated(UInt256 blockHash)
        {
            var handler = this.BlockInvalidated;
            if (handler != null)
                handler(blockHash);
        }

        private object GetBlockLock(UInt256 blockHash)
        {
            return this.presentBlockTxesLocks[Math.Abs(blockHash.GetHashCode()) % this.presentBlockTxesLocks.Length];
        }
    }
}
