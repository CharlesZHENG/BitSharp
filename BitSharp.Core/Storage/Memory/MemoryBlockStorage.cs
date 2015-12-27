using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Threading;

namespace BitSharp.Core.Storage.Memory
{
    public class MemoryBlockStorage : IBlockStorage
    {
        private readonly ConcurrentDictionary<UInt256, ChainedHeader> chainedHeaders;
        private readonly ConcurrentSet<UInt256> invalidBlocks;
        private readonly SortedDictionary<BigInteger, List<ChainedHeader>> chainedHeadersByTotalWork;
        private readonly ReaderWriterLockSlim totalWorkLock = new ReaderWriterLockSlim();

        private bool disposed;

        public MemoryBlockStorage()
        {
            this.chainedHeaders = new ConcurrentDictionary<UInt256, ChainedHeader>();
            this.invalidBlocks = new ConcurrentSet<UInt256>();
            this.chainedHeadersByTotalWork = new SortedDictionary<BigInteger, List<ChainedHeader>>(new ReverseBigIntegerComparer());
        }

        private class ReverseBigIntegerComparer : IComparer<BigInteger>
        {
            public int Compare(BigInteger x, BigInteger y)
            {
                return -(x.CompareTo(y));
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
                totalWorkLock.Dispose();

                disposed = true;
            }
        }

        public bool ContainsChainedHeader(UInt256 blockHash)
        {
            return this.chainedHeaders.ContainsKey(blockHash);
        }

        public bool TryAddChainedHeader(ChainedHeader chainedHeader)
        {
            return totalWorkLock.DoWrite(() =>
            {
                if (this.chainedHeaders.TryAdd(chainedHeader.Hash, chainedHeader))
                {
                    List<ChainedHeader> headersAtTotalWork;
                    if (!chainedHeadersByTotalWork.TryGetValue(chainedHeader.TotalWork, out headersAtTotalWork))
                    {
                        headersAtTotalWork = new List<ChainedHeader>();
                        chainedHeadersByTotalWork.Add(chainedHeader.TotalWork, headersAtTotalWork);
                    }
                    headersAtTotalWork.Add(chainedHeader);
                    return true;
                }
                else
                {
                    return false;
                }
            });
        }

        public bool TryGetChainedHeader(UInt256 blockHash, out ChainedHeader chainedHeader)
        {
            return this.chainedHeaders.TryGetValue(blockHash, out chainedHeader);
        }

        public bool TryRemoveChainedHeader(UInt256 blockHash)
        {
            return totalWorkLock.DoWrite(() =>
            {
                ChainedHeader chainedHeader;
                if (this.chainedHeaders.TryRemove(blockHash, out chainedHeader))
                {
                    var headersAtTotalWork = chainedHeadersByTotalWork[chainedHeader.TotalWork];
                    headersAtTotalWork.Remove(chainedHeader);
                    if (headersAtTotalWork.Count == 0)
                        chainedHeadersByTotalWork.Remove(chainedHeader.TotalWork);

                    return true;
                }
                else
                    return false;
            });
        }

        public ChainedHeader FindMaxTotalWork()
        {
            return totalWorkLock.DoRead(() =>
            {
                var maxTotalWork = BigInteger.Zero;
                var candidateHeaders = new List<ChainedHeader>();
                var finished = false;

                foreach (var totalWork in chainedHeadersByTotalWork.Keys)
                {
                    var headersAtTotalWork = chainedHeadersByTotalWork[totalWork];
                    foreach (var chainedHeader in headersAtTotalWork)
                    {
                        // check if this block is valid
                        if (!invalidBlocks.Contains(chainedHeader.Hash))
                        {
                            // initialize max total work, if it isn't yet
                            if (maxTotalWork == BigInteger.Zero)
                                maxTotalWork = chainedHeader.TotalWork;

                            // add this header as a candidate if it ties the max total work
                            if (chainedHeader.TotalWork >= maxTotalWork)
                                candidateHeaders.Add(chainedHeader);
                            else
                            {
                                finished = true;
                                break;
                            }
                        }
                    }

                    if (finished)
                        break;
                }

                // take the earliest header seen with the max total work
                candidateHeaders.Sort((left, right) => left.DateSeen.CompareTo(right.DateSeen));
                return candidateHeaders.FirstOrDefault();
            });
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
