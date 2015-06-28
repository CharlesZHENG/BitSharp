using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;

namespace BitSharp.Core.Storage.Memory
{
    internal class MemoryChainStateStorage : IDisposable
    {
        private readonly object lockObject = new object();
        private readonly SemaphoreSlim writeTxLock = new SemaphoreSlim(1);

        private ChainedHeader chainTip;
        private int unspentTxCount;
        private int unspentOutputCount;
        private int totalTxCount;
        private int totalInputCount;
        private int totalOutputCount;
        private ImmutableSortedDictionary<UInt256, UnspentTx>.Builder unspentTransactions;
        private ImmutableDictionary<int, IImmutableList<UInt256>>.Builder blockSpentTxes;
        private ImmutableDictionary<UInt256, IImmutableList<UnmintedTx>>.Builder blockUnmintedTxes;

        private long chainTipVersion;
        private long unspentTxCountVersion;
        private long unspentOutputCountVersion;
        private long totalTxCountVersion;
        private long totalInputCountVersion;
        private long totalOutputCountVersion;
        private long unspentTxesVersion;
        private long blockSpentTxesVersion;
        private long blockUnmintedTxesVersion;

        public MemoryChainStateStorage(ChainedHeader chainTip = null, int? unspentTxCount = null, int? totalTxCount = null, int? totalInputCount = null, int? totalOutputCount = null, int? unspentOutputCount = null, ImmutableSortedDictionary<UInt256, UnspentTx> unspentTransactions = null, ImmutableDictionary<int, IImmutableList<UInt256>> blockSpentTxes = null, ImmutableDictionary<UInt256, IImmutableList<UnmintedTx>> blockUnmintedTxes = null)
        {
            this.chainTip = chainTip;
            this.unspentTxCount = unspentTxCount ?? 0;
            this.unspentOutputCount = unspentOutputCount ?? 0;
            this.totalTxCount = totalTxCount ?? 0;
            this.totalInputCount = totalInputCount ?? 0;
            this.totalOutputCount = totalOutputCount ?? 0;
            this.unspentTransactions = unspentTransactions != null ? unspentTransactions.ToBuilder() : ImmutableSortedDictionary.CreateBuilder<UInt256, UnspentTx>();
            this.blockSpentTxes = blockSpentTxes != null ? blockSpentTxes.ToBuilder() : ImmutableDictionary.CreateBuilder<int, IImmutableList<UInt256>>();
            this.blockUnmintedTxes = blockUnmintedTxes != null ? blockUnmintedTxes.ToBuilder() : ImmutableDictionary.CreateBuilder<UInt256, IImmutableList<UnmintedTx>>();
        }

        public void Dispose()
        {
            this.writeTxLock.Dispose();
        }

        internal SemaphoreSlim WriteTxLock { get { return this.writeTxLock; } }

        public void BeginTransaction(out ChainedHeader chainTip, out int? unspentTxCount, out int? unspentOutputCount, out int? totalTxCount, out int? totalInputCount, out int? totalOutputCount, out ImmutableSortedDictionary<UInt256, UnspentTx>.Builder unspentTransactions, out ImmutableDictionary<int, IImmutableList<UInt256>>.Builder blockSpentTxes, out ImmutableDictionary<UInt256, IImmutableList<UnmintedTx>>.Builder blockUnmintedTxes, out long chainTipVersion, out long unspentTxCountVersion, out long unspentOutputCountVersion, out long totalTxCountVersion, out long totalInputCountVersion, out long totalOutputCountVersion, out long unspentTxesVersion, out long blockSpentTxesVersion, out long blockUnmintedTxesVersion)
        {
            lock (this.lockObject)
            {
                chainTip = this.chainTip;
                unspentTxCount = this.unspentTxCount;
                unspentOutputCount = this.unspentOutputCount;
                totalTxCount = this.totalTxCount;
                totalInputCount = this.totalInputCount;
                totalOutputCount = this.totalOutputCount;
                unspentTransactions = this.unspentTransactions.ToImmutable().ToBuilder();
                blockSpentTxes = this.blockSpentTxes.ToImmutable().ToBuilder();
                blockUnmintedTxes = this.blockUnmintedTxes.ToImmutable().ToBuilder();

                chainTipVersion = this.chainTipVersion;
                unspentTxCountVersion = this.unspentTxCountVersion;
                unspentOutputCountVersion = this.unspentOutputCountVersion;
                totalTxCountVersion = this.totalTxCountVersion;
                totalInputCountVersion = this.totalInputCountVersion;
                totalOutputCountVersion = this.totalOutputCountVersion;
                unspentTxesVersion = this.unspentTxesVersion;
                blockSpentTxesVersion = this.blockSpentTxesVersion;
                blockUnmintedTxesVersion = this.blockUnmintedTxesVersion;
            }
        }

        public void CommitTransaction(ChainedHeader chainTip, int? unspentTxCount, int? unspentOutputCount, int? totalTxCount, int? totalInputCount, int? totalOutputCount, ImmutableSortedDictionary<UInt256, UnspentTx>.Builder unspentTransactions, ImmutableDictionary<int, IImmutableList<UInt256>>.Builder blockSpentTxes, ImmutableDictionary<UInt256, IImmutableList<UnmintedTx>>.Builder blockUnmintedTxes, long chainVersion, long unspentTxCountVersion, long unspentOutputCountVersion, long totalTxCountVersion, long totalInputCountVersion, long totalOutputCountVersion, long unspentTxesVersion, long blockSpentTxesVersion, long blockUnmintedTxesVersion)
        {
            lock (this.lockObject)
            {
                if (chainTip != null && this.chainTipVersion != chainVersion
                    || unspentTxCount != null && unspentTxCountVersion != this.unspentTxCountVersion
                    || unspentOutputCount != null && unspentOutputCountVersion != this.unspentOutputCountVersion
                    || totalTxCount != null && totalTxCountVersion != this.totalTxCountVersion
                    || totalInputCount != null && totalInputCountVersion != this.totalInputCountVersion
                    || totalOutputCount != null && totalOutputCountVersion != this.totalOutputCountVersion
                    || unspentTransactions != null && unspentTxesVersion != this.unspentTxesVersion
                    || blockSpentTxes != null && blockSpentTxesVersion != this.blockSpentTxesVersion
                    || blockUnmintedTxes != null && blockUnmintedTxesVersion != this.blockUnmintedTxesVersion)
                    throw new InvalidOperationException();

                if (chainTip != null)
                {
                    this.chainTip = chainTip;
                    this.chainTipVersion++;
                }

                if (unspentTxCount != null)
                {
                    this.unspentTxCount = unspentTxCount.Value;
                    this.unspentTxCountVersion++;
                }

                if (unspentOutputCount != null)
                {
                    this.unspentOutputCount = unspentOutputCount.Value;
                    this.unspentOutputCountVersion++;
                }

                if (totalTxCount != null)
                {
                    this.totalTxCount = totalTxCount.Value;
                    this.totalTxCountVersion++;
                }

                if (totalInputCount != null)
                {
                    this.totalInputCount = totalInputCount.Value;
                    this.totalInputCountVersion++;
                }

                if (totalOutputCount != null)
                {
                    this.totalOutputCount = totalOutputCount.Value;
                    this.totalOutputCountVersion++;
                }

                if (unspentTransactions != null)
                {
                    this.unspentTransactions = unspentTransactions.ToImmutable().ToBuilder();
                    this.unspentTxesVersion++;
                }

                if (blockSpentTxes != null)
                {
                    this.blockSpentTxes = blockSpentTxes.ToImmutable().ToBuilder();
                    this.blockSpentTxesVersion++;
                }

                if (blockUnmintedTxes != null)
                {
                    this.blockUnmintedTxes = blockUnmintedTxes.ToImmutable().ToBuilder();
                    this.blockUnmintedTxesVersion++;
                }
            }
        }

        public ChainedHeader ChainTip
        {
            get
            {
                lock (this.lockObject)
                    return this.chainTip;
            }
            set
            {
                lock (this.lockObject)
                {
                    this.chainTip = value;
                    this.chainTipVersion++;
                }
            }
        }

        public int UnspentTxCount
        {
            get
            {
                lock (this.lockObject)
                    return this.unspentTxCount;
            }
            set
            {
                lock (this.lockObject)
                {
                    this.unspentTxCount = value;
                    this.unspentTxCountVersion++;
                }
            }
        }

        public int UnspentOutputCount
        {
            get
            {
                lock (this.lockObject)
                    return this.unspentOutputCount;
            }
            set
            {
                lock (this.lockObject)
                {
                    this.unspentOutputCount = value;
                    this.unspentOutputCountVersion++;
                }
            }
        }

        public int TotalTxCount
        {
            get
            {
                lock (this.lockObject)
                    return this.totalTxCount;
            }
            set
            {
                lock (this.lockObject)
                {
                    this.totalTxCount = value;
                    this.totalTxCountVersion++;
                }
            }
        }

        public int TotalInputCount
        {
            get
            {
                lock (this.lockObject)
                    return this.totalInputCount;
            }
            set
            {
                lock (this.lockObject)
                {
                    this.totalInputCount = value;
                    this.totalInputCountVersion++;
                }
            }
        }

        public int TotalOutputCount
        {
            get
            {
                lock (this.lockObject)
                    return this.totalOutputCount;
            }
            set
            {
                lock (this.lockObject)
                {
                    this.totalOutputCount = value;
                    this.totalOutputCountVersion++;
                }
            }
        }

        public bool ContainsUnspentTx(UInt256 txHash)
        {
            lock (this.lockObject)
                return this.unspentTransactions.ContainsKey(txHash);
        }

        public bool TryGetUnspentTx(UInt256 txHash, out UnspentTx unspentTx)
        {
            lock (this.lockObject)
                return this.unspentTransactions.TryGetValue(txHash, out unspentTx);
        }

        public bool TryAddUnspentTx(UnspentTx unspentTx)
        {
            lock (this.lockObject)
            {
                var wasAdded = this.unspentTransactions.TryAdd(unspentTx.TxHash, unspentTx);
                if (wasAdded)
                    this.unspentTxesVersion++;

                return wasAdded;
            }
        }

        public bool TryRemoveUnspentTx(UInt256 txHash)
        {
            lock (this.lockObject)
            {
                var wasRemoved = this.unspentTransactions.Remove(txHash);
                if (wasRemoved)
                    this.unspentTxesVersion++;

                return wasRemoved;
            }
        }

        public bool TryUpdateUnspentTx(UnspentTx unspentTx)
        {
            lock (this.lockObject)
            {
                if (this.unspentTransactions.ContainsKey(unspentTx.TxHash))
                {
                    this.unspentTransactions[unspentTx.TxHash] = unspentTx;
                    this.unspentTxesVersion++;
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

        public IEnumerable<UnspentTx> ReadUnspentTransactions()
        {
            lock (this.lockObject)
                return this.unspentTransactions.ToImmutable().Values;
        }

        public bool ContainsBlockSpentTxes(int blockIndex)
        {
            lock (this.lockObject)
                return this.blockSpentTxes.ContainsKey(blockIndex);
        }


        public bool TryGetBlockSpentTxes(int blockIndex, out IImmutableList<UInt256> spentTxes)
        {
            lock (this.lockObject)
            {
                return this.blockSpentTxes.TryGetValue(blockIndex, out spentTxes);
            }
        }

        public bool TryAddBlockSpentTxes(int blockIndex, IImmutableList<UInt256> spentTxes)
        {
            lock (this.lockObject)
            {
                try
                {
                    this.blockSpentTxes.Add(blockIndex, ImmutableArray.CreateRange(spentTxes));
                    this.blockSpentTxesVersion++;
                    return true;
                }
                catch (ArgumentException)
                {
                    return false;
                }
            }
        }

        public bool TryRemoveBlockSpentTxes(int blockIndex)
        {
            lock (this.lockObject)
            {
                var wasRemoved = this.blockSpentTxes.Remove(blockIndex);
                if (wasRemoved)
                    this.blockSpentTxesVersion++;

                return wasRemoved;
            }
        }

        public bool ContainsBlockUnmintedTxes(UInt256 blockHash)
        {
            lock (this.lockObject)
                return this.blockUnmintedTxes.ContainsKey(blockHash);
        }

        public bool TryGetBlockUnmintedTxes(UInt256 blockHash, out IImmutableList<UnmintedTx> unmintedTxes)
        {
            lock (this.lockObject)
                return this.blockUnmintedTxes.TryGetValue(blockHash, out unmintedTxes);
        }

        public bool TryAddBlockUnmintedTxes(UInt256 blockHash, IImmutableList<UnmintedTx> unmintedTxes)
        {
            lock (this.lockObject)
            {
                try
                {
                    this.blockUnmintedTxes.Add(blockHash, ImmutableArray.CreateRange(unmintedTxes));
                    this.blockUnmintedTxesVersion++;
                    return true;
                }
                catch (ArgumentException)
                {
                    return false;
                }
            }
        }

        public bool TryRemoveBlockUnmintedTxes(UInt256 blockHash)
        {
            lock (this.lockObject)
            {
                var wasRemoved = this.blockUnmintedTxes.Remove(blockHash);
                if (wasRemoved)
                    this.blockUnmintedTxesVersion++;

                return wasRemoved;
            }
        }

        public void Defragment()
        {
        }
    }
}
