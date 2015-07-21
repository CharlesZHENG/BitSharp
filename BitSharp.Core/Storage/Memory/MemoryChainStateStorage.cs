using BitSharp.Common;
using BitSharp.Core.Domain;
using System;
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
        private ImmutableSortedDictionary<UInt256, ChainedHeader>.Builder headers;
        private ImmutableSortedDictionary<UInt256, UnspentTx>.Builder unspentTransactions;
        private ImmutableDictionary<int, BlockSpentTxes>.Builder blockSpentTxes;
        private ImmutableDictionary<UInt256, IImmutableList<UnmintedTx>>.Builder blockUnmintedTxes;

        private long chainTipVersion;
        private long unspentTxCountVersion;
        private long unspentOutputCountVersion;
        private long totalTxCountVersion;
        private long totalInputCountVersion;
        private long totalOutputCountVersion;
        private long headersVersion;
        private long unspentTxesVersion;
        private long blockSpentTxesVersion;
        private long blockUnmintedTxesVersion;

        public MemoryChainStateStorage(ChainedHeader chainTip = null, int? unspentTxCount = null, int? totalTxCount = null, int? totalInputCount = null, int? totalOutputCount = null, int? unspentOutputCount = null, ImmutableSortedDictionary<UInt256, ChainedHeader> headers = null, ImmutableSortedDictionary<UInt256, UnspentTx> unspentTransactions = null, ImmutableDictionary<int, BlockSpentTxes> blockSpentTxes = null, ImmutableDictionary<UInt256, IImmutableList<UnmintedTx>> blockUnmintedTxes = null)
        {
            this.chainTip = chainTip;
            this.unspentTxCount = unspentTxCount ?? 0;
            this.unspentOutputCount = unspentOutputCount ?? 0;
            this.totalTxCount = totalTxCount ?? 0;
            this.totalInputCount = totalInputCount ?? 0;
            this.totalOutputCount = totalOutputCount ?? 0;
            this.headers = headers != null ? headers.ToBuilder() : ImmutableSortedDictionary.CreateBuilder<UInt256, ChainedHeader>();
            this.unspentTransactions = unspentTransactions != null ? unspentTransactions.ToBuilder() : ImmutableSortedDictionary.CreateBuilder<UInt256, UnspentTx>();
            this.blockSpentTxes = blockSpentTxes != null ? blockSpentTxes.ToBuilder() : ImmutableDictionary.CreateBuilder<int, BlockSpentTxes>();
            this.blockUnmintedTxes = blockUnmintedTxes != null ? blockUnmintedTxes.ToBuilder() : ImmutableDictionary.CreateBuilder<UInt256, IImmutableList<UnmintedTx>>();
        }

        public void Dispose()
        {
            this.writeTxLock.Dispose();
        }

        internal SemaphoreSlim WriteTxLock { get { return this.writeTxLock; } }

        public void BeginTransaction(out ChainedHeader chainTip, out int? unspentTxCount, out int? unspentOutputCount, out int? totalTxCount, out int? totalInputCount, out int? totalOutputCount, out ImmutableSortedDictionary<UInt256, ChainedHeader>.Builder headers, out ImmutableSortedDictionary<UInt256, UnspentTx>.Builder unspentTransactions, out ImmutableDictionary<int, BlockSpentTxes>.Builder blockSpentTxes, out ImmutableDictionary<UInt256, IImmutableList<UnmintedTx>>.Builder blockUnmintedTxes, out long chainTipVersion, out long unspentTxCountVersion, out long unspentOutputCountVersion, out long totalTxCountVersion, out long totalInputCountVersion, out long totalOutputCountVersion, out long headersVersion, out long unspentTxesVersion, out long blockSpentTxesVersion, out long blockUnmintedTxesVersion)
        {
            lock (this.lockObject)
            {
                chainTip = this.chainTip;
                unspentTxCount = this.unspentTxCount;
                unspentOutputCount = this.unspentOutputCount;
                totalTxCount = this.totalTxCount;
                totalInputCount = this.totalInputCount;
                totalOutputCount = this.totalOutputCount;
                headers = this.headers.ToImmutable().ToBuilder();
                unspentTransactions = this.unspentTransactions.ToImmutable().ToBuilder();
                blockSpentTxes = this.blockSpentTxes.ToImmutable().ToBuilder();
                blockUnmintedTxes = this.blockUnmintedTxes.ToImmutable().ToBuilder();

                chainTipVersion = this.chainTipVersion;
                unspentTxCountVersion = this.unspentTxCountVersion;
                unspentOutputCountVersion = this.unspentOutputCountVersion;
                totalTxCountVersion = this.totalTxCountVersion;
                totalInputCountVersion = this.totalInputCountVersion;
                totalOutputCountVersion = this.totalOutputCountVersion;
                headersVersion = this.headersVersion;
                unspentTxesVersion = this.unspentTxesVersion;
                blockSpentTxesVersion = this.blockSpentTxesVersion;
                blockUnmintedTxesVersion = this.blockUnmintedTxesVersion;
            }
        }

        public void CommitTransaction(ChainedHeader chainTip, int? unspentTxCount, int? unspentOutputCount, int? totalTxCount, int? totalInputCount, int? totalOutputCount, ImmutableSortedDictionary<UInt256, ChainedHeader>.Builder headers, ImmutableSortedDictionary<UInt256, UnspentTx>.Builder unspentTransactions, ImmutableDictionary<int, BlockSpentTxes>.Builder blockSpentTxes, ImmutableDictionary<UInt256, IImmutableList<UnmintedTx>>.Builder blockUnmintedTxes, long chainVersion, long unspentTxCountVersion, long unspentOutputCountVersion, long totalTxCountVersion, long totalInputCountVersion, long totalOutputCountVersion, long headersVersion, long unspentTxesVersion, long blockSpentTxesVersion, long blockUnmintedTxesVersion)
        {
            lock (this.lockObject)
            {
                if (chainTip != null && this.chainTipVersion != chainVersion
                    || unspentTxCount != null && unspentTxCountVersion != this.unspentTxCountVersion
                    || unspentOutputCount != null && unspentOutputCountVersion != this.unspentOutputCountVersion
                    || totalTxCount != null && totalTxCountVersion != this.totalTxCountVersion
                    || totalInputCount != null && totalInputCountVersion != this.totalInputCountVersion
                    || totalOutputCount != null && totalOutputCountVersion != this.totalOutputCountVersion
                    || headers != null && headersVersion != this.headersVersion
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

                if (headers != null)
                {
                    this.headers = headers.ToImmutable().ToBuilder();
                    this.headersVersion++;
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
    }
}
