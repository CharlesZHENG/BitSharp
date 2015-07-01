using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Caching;
using System.Threading;

namespace BitSharp.Core.Storage
{
    public class CoreStorage : ICoreStorage, IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly IStorageManager storageManager;
        private readonly IBlockStorage blockStorage;
        private readonly IBlockTxesStorage blockTxesStorage;

        private readonly ConcurrentDictionary<UInt256, ChainedHeader> cachedHeaders;

        private readonly ConcurrentDictionary<UInt256, bool> presentBlockTxes = new ConcurrentDictionary<UInt256, bool>();
        private readonly object[] presentBlockTxesLocks = new object[64];

        private readonly DurationMeasure txLoadDurationMeasure = new DurationMeasure(TimeSpan.FromMinutes(5), TimeSpan.FromSeconds(5));
        private readonly RateMeasure txLoadRateMeasure = new RateMeasure(TimeSpan.FromMinutes(5), TimeSpan.FromSeconds(5));

        private bool isDisposed;

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
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!isDisposed && disposing)
            {
                this.txLoadDurationMeasure.Dispose();
                this.txLoadRateMeasure.Dispose();

                isDisposed = true;
            }
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
                        logger.Warn("Unexpected condition: validly chained header could not be added");
                    }
                }
            }

            chainedHeader = default(ChainedHeader);
            return false;
        }

        public bool TryReadChain(UInt256 blockHash, out Chain chain)
        {
            // return an empty chain for null blockHash
            // when retrieving a chain by its tip, a null tip represents an empty chain
            if (blockHash == null)
            {
                chain = new ChainBuilder().ToImmutable();
                return true;
            }

            var retrievedHeaders = new List<ChainedHeader>();

            ChainedHeader chainedHeader;
            if (TryGetChainedHeader(blockHash, out chainedHeader))
            {
                var expectedHeight = chainedHeader.Height;
                do
                {
                    if (chainedHeader.Height != expectedHeight)
                    {
                        chain = default(Chain);
                        return false;
                    }

                    retrievedHeaders.Add(chainedHeader);
                    expectedHeight--;
                }
                while (expectedHeight >= 0 && chainedHeader.PreviousBlockHash != chainedHeader.Hash
                    && TryGetChainedHeader(chainedHeader.PreviousBlockHash, out chainedHeader));

                if (retrievedHeaders.Last().Height != 0)
                {
                    chain = default(Chain);
                    return false;
                }

                var chainBuilder = new ChainBuilder();
                for (var i = retrievedHeaders.Count - 1; i >= 0; i--)
                    chainBuilder.AddBlock(retrievedHeaders[i]);

                chain = chainBuilder.ToImmutable();
                return true;
            }
            else
            {
                chain = default(Chain);
                return false;
            }
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
            var stopwatch = Stopwatch.StartNew();
            if (this.blockTxesStorage.TryGetTransaction(blockHash, txIndex, out transaction))
            {
                stopwatch.Stop();
                this.txLoadDurationMeasure.Tick(stopwatch.Elapsed);
                this.txLoadRateMeasure.Tick();
                return true;
            }
            else
                return false;
        }

        public float GetTxLoadRate()
        {
            return this.txLoadRateMeasure.GetAverage();
        }

        public TimeSpan GetTxLoadDuration()
        {
            return this.txLoadDurationMeasure.GetAverage();
        }

        public bool TryReadBlockTransactions(UInt256 blockHash, UInt256 merkleRoot, bool requireTransactions, out IEnumerable<BlockTx> blockTxes)
        {
            IEnumerator<BlockTx> rawBlockTxes;
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

        private IEnumerable<BlockTx> ReadBlockTransactions(UInt256 blockHash, UInt256 merkleRoot, bool requireTransactions, IEnumerator<BlockTx> blockTxes)
        {
            //TODO merkle validation should happen in BlockValidator
            using (var blockTxesEnumerator = MerkleTree.ReadMerkleTreeNodes(merkleRoot, blockTxes.UsingAsEnumerable()).GetEnumerator())
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
