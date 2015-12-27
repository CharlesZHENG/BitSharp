using BitSharp.Common;
using BitSharp.Core.Domain;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;

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
            var maxTotalWork = BigInteger.Zero;
            var candidateHeaders = new SortedList<DateTime, ChainedHeader>();

            // TODO more efficient memory implementation than scanning every time, use a sorted dictionary
            foreach (var chainedHeader in this.chainedHeaders.Values)
            {
                // check if this block is valid
                if (!invalidBlocks.Contains(chainedHeader.Hash))
                {
                    // initialize max total work, if it isn't yet
                    if (maxTotalWork == BigInteger.Zero)
                        maxTotalWork = chainedHeader.TotalWork;

                    // if this header exceeds max total work, set it as the new max
                    if (chainedHeader.TotalWork > maxTotalWork)
                    {
                        maxTotalWork = chainedHeader.TotalWork;
                        candidateHeaders.Clear();
                        candidateHeaders.Add(chainedHeader.DateSeen, chainedHeader);
                    }
                    // else add this header as a candidate if it ties the max total work
                    else if (chainedHeader.TotalWork == maxTotalWork)
                        candidateHeaders.Add(chainedHeader.DateSeen, chainedHeader);
                }
            }

            // take the earliest header seen with the max total work
            return candidateHeaders.Values.FirstOrDefault();
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
