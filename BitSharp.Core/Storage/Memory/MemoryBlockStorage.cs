using BitSharp.Common;
using BitSharp.Core.Domain;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace BitSharp.Core.Storage.Memory
{
    public class MemoryBlockStorage : IBlockStorage
    {
        private readonly ConcurrentDictionary<UInt256, ChainedHeader> chainedHeaders;
        private readonly ConcurrentSet<UInt256> invalidBlocks;

        public MemoryBlockStorage()
        {
            this.chainedHeaders = new ConcurrentDictionary<UInt256, ChainedHeader>();
            this.invalidBlocks = new ConcurrentSet<UInt256>();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
        }

        public bool ContainsChainedHeader(UInt256 blockHash)
        {
            return this.chainedHeaders.ContainsKey(blockHash);
        }

        public bool TryAddChainedHeader(ChainedHeader chainedHeader)
        {
            return this.chainedHeaders.TryAdd(chainedHeader.Hash, chainedHeader);
        }

        public bool TryGetChainedHeader(UInt256 blockHash, out ChainedHeader chainedHeader)
        {
            return this.chainedHeaders.TryGetValue(blockHash, out chainedHeader);
        }

        public bool TryRemoveChainedHeader(UInt256 blockHash)
        {
            ChainedHeader chainedHeader;
            return this.chainedHeaders.TryRemove(blockHash, out chainedHeader);
        }

        public ChainedHeader FindMaxTotalWork()
        {
            ChainedHeader maxTotalWorkHeader = null;

            foreach (var chainedHeader in this.chainedHeaders.Values)
            {
                if (!invalidBlocks.Contains(chainedHeader.Hash)
                    && (maxTotalWorkHeader == null || chainedHeader.TotalWork > maxTotalWorkHeader.TotalWork))
                {
                    maxTotalWorkHeader = chainedHeader;
                }
            }

            return maxTotalWorkHeader;
        }

        public IEnumerable<ChainedHeader> ReadChainedHeaders()
        {
            return this.chainedHeaders.Values;
        }

        public bool IsBlockInvalid(UInt256 blockHash)
        {
            return this.invalidBlocks.Contains(blockHash);
        }

        public void MarkBlockInvalid(UInt256 blockHash)
        {
            this.invalidBlocks.Add(blockHash);
        }

        public void Flush()
        {
        }

        public void Defragment()
        {
        }
    }
}
