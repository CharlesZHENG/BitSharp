using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Core.Storage
{
    public class CoreStorage : ICoreStorage, IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly IStorageManager storageManager;
        private readonly Lazy<IBlockStorage> blockStorage;
        private readonly Lazy<IBlockTxesStorage> blockTxesStorage;

        private readonly Lazy<ConcurrentDictionary<UInt256, ChainedHeader>> cachedHeaders;

        private readonly ConcurrentDictionary<UInt256, bool> presentBlockTxes = new ConcurrentDictionary<UInt256, bool>();
        private readonly object[] presentBlockTxesLocks = new object[64];

        private bool isDisposed;

        public CoreStorage(IStorageManager storageManager)
        {
            for (var i = 0; i < this.presentBlockTxesLocks.Length; i++)
                presentBlockTxesLocks[i] = new object();

            this.storageManager = storageManager;
            this.blockStorage = new Lazy<IBlockStorage>(() => storageManager.BlockStorage);
            this.blockTxesStorage = new Lazy<IBlockTxesStorage>(() => storageManager.BlockTxesStorage);

            this.cachedHeaders = new Lazy<ConcurrentDictionary<UInt256, ChainedHeader>>(() =>
            {
                var cachedHeaders = new ConcurrentDictionary<UInt256, ChainedHeader>();
                foreach (var chainedHeader in this.blockStorage.Value.ReadChainedHeaders())
                    cachedHeaders[chainedHeader.Hash] = chainedHeader;

                return cachedHeaders;
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
                isDisposed = true;
            }
        }

        public event Action<ChainedHeader> ChainedHeaderAdded;

        public event Action<ChainedHeader> BlockTxesAdded;

        public event Action<ChainedHeader> BlockTxesRemoved;

        public event Action<UInt256> BlockInvalidated;

        public int ChainedHeaderCount => -1;

        public int BlockWithTxesCount => this.blockTxesStorage.Value.BlockCount;

        internal IStorageManager StorageManager => this.storageManager;

        public bool ContainsChainedHeader(UInt256 blockHash)
        {
            ChainedHeader chainedHeader;
            return TryGetChainedHeader(blockHash, out chainedHeader);
        }

        public void AddGenesisBlock(ChainedHeader genesisHeader)
        {
            if (genesisHeader.Height != 0)
                throw new ArgumentException("genesisHeader");

            if (this.blockStorage.Value.TryAddChainedHeader(genesisHeader))
            {
                this.cachedHeaders.Value[genesisHeader.Hash] = genesisHeader;
                RaiseChainedHeaderAdded(genesisHeader);
            }
        }

        public bool TryGetChainedHeader(UInt256 blockHash, out ChainedHeader chainedHeader)
        {
            if (this.cachedHeaders.Value.TryGetValue(blockHash, out chainedHeader))
            {
                return chainedHeader != null;
            }
            else if (this.blockStorage.Value.TryGetChainedHeader(blockHash, out chainedHeader))
            {
                this.cachedHeaders.Value[blockHash] = chainedHeader;
                return true;
            }
            else
            {
                this.cachedHeaders.Value[blockHash] = null;
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
                    chainedHeader = ChainedHeader.CreateFromPrev(previousChainedHeader, blockHeader, DateTime.Now);
                    if (chainedHeader == null)
                        return false;

                    if (this.blockStorage.Value.TryAddChainedHeader(chainedHeader))
                    {
                        this.cachedHeaders.Value[chainedHeader.Hash] = chainedHeader;

                        if (!suppressEvent)
                            RaiseChainedHeaderAdded(chainedHeader);

                        return true;
                    }
                    else
                    {
                        logger.Warn("Unexpected condition: validly chained header could not be added");
                    }
                }
            }

            chainedHeader = default(ChainedHeader);
            return false;
        }

        public bool TryReadChain(UInt256 blockHash, out Chain chain)
        {
            return Chain.TryReadChain(blockHash, out chain,
                headerHash =>
                {
                    ChainedHeader chainedHeader;
                    TryGetChainedHeader(headerHash, out chainedHeader);
                    return chainedHeader;
                });
        }

        public ChainedHeader FindMaxTotalWork()
        {
            return this.blockStorage.Value.FindMaxTotalWork();
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
                    present = this.blockTxesStorage.Value.ContainsBlock(blockHash);
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
                    if (this.blockTxesStorage.Value.TryAddBlockTransactions(block.Hash, block.BlockTxes))
                    {
                        this.presentBlockTxes[block.Hash] = true;
                        RaiseBlockTxesAdded(chainedHeader);
                        return true;
                    }
                }

                return false;
            };
        }

        public void AddBlocks(IEnumerable<Block> blocks)
        {
            foreach (var block in blocks)
                TryAddBlock(block);
        }

        public bool TryGetBlock(UInt256 blockHash, out Block block)
        {
            ChainedHeader chainedHeader;
            if (!TryGetChainedHeader(blockHash, out chainedHeader))
            {
                block = default(Block);
                return false;
            }

            IEnumerator<BlockTx> blockTxes;
            if (TryReadBlockTransactions(chainedHeader.Hash, out blockTxes))
            {
                block = new Block(chainedHeader.BlockHeader, blockTxes.UsingAsEnumerable().ToImmutableArray());
                return true;
            }
            else
            {
                block = default(Block);
                return false;
            }
        }

        public bool TryGetTransaction(UInt256 blockHash, int txIndex, out BlockTx transaction)
        {
            return this.blockTxesStorage.Value.TryGetTransaction(blockHash, txIndex, out transaction);
        }

        public bool TryReadBlockTransactions(UInt256 blockHash, out IEnumerator<BlockTx> blockTxes)
        {
            IEnumerator<BlockTx> rawBlockTxes;
            if (this.blockTxesStorage.Value.TryReadBlockTransactions(blockHash, out rawBlockTxes))
            {
                blockTxes = ReadBlockTransactions(blockHash, rawBlockTxes);
                return true;
            }
            else
            {
                blockTxes = null;
                return false;
            }
        }

        private IEnumerator<BlockTx> ReadBlockTransactions(UInt256 blockHash, IEnumerator<BlockTx> blockTxes)
        {
            using (blockTxes)
            {
                while (true)
                {
                    bool read;
                    try
                    {
                        read = blockTxes.MoveNext();
                    }
                    catch (MissingDataException e)
                    {
                        var missingBlockHash = (UInt256)e.Key;

                        lock (GetBlockLock(blockHash))
                            this.presentBlockTxes[missingBlockHash] = false;

                        throw;
                    }

                    if (read)
                        yield return blockTxes.Current;
                    else
                        yield break;
                }
            }
        }

        public bool IsBlockInvalid(UInt256 blockHash)
        {
            return this.blockStorage.Value.IsBlockInvalid(blockHash);
        }

        //TODO this should mark any blocks chained on top as invalid
        public void MarkBlockInvalid(UInt256 blockHash, Chain targetChain)
        {
            var invalidatedBlocks = new List<UInt256>();
            try
            {
                this.blockStorage.Value.MarkBlockInvalid(blockHash);
                invalidatedBlocks.Add(blockHash);

                // mark any blocks further in the target chain as invalid
                ChainedHeader invalidBlock;
                if (targetChain.BlocksByHash.TryGetValue(blockHash, out invalidBlock))
                {
                    for (var height = invalidBlock.Height; height <= targetChain.Height; height++)
                    {
                        invalidBlock = targetChain.Blocks[height];
                        this.blockStorage.Value.MarkBlockInvalid(invalidBlock.Hash);
                        invalidatedBlocks.Add(blockHash);
                    }
                }
            }
            finally
            {
                foreach (var invalidatedBlock in invalidatedBlocks)
                    BlockInvalidated?.Invoke(invalidatedBlock);
            }
        }

        private void RaiseChainedHeaderAdded(ChainedHeader chainedHeader)
        {
            this.ChainedHeaderAdded?.Invoke(chainedHeader);
        }

        private void RaiseBlockTxesAdded(ChainedHeader chainedHeader)
        {
            this.BlockTxesAdded?.Invoke(chainedHeader);
        }

        private void RaiseBlockTxesRemoved(ChainedHeader chainedHeader)
        {
            this.BlockTxesRemoved?.Invoke(chainedHeader);
        }

        private object GetBlockLock(UInt256 blockHash)
        {
            return this.presentBlockTxesLocks[Math.Abs(blockHash.GetHashCode()) % this.presentBlockTxesLocks.Length];
        }
    }
}
