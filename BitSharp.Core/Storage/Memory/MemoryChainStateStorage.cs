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

        private CommittedRecord<ChainedHeader> chainTip;
        private CommittedRecord<int> unspentTxCount;
        private CommittedRecord<int> unspentOutputCount;
        private CommittedRecord<int> totalTxCount;
        private CommittedRecord<int> totalInputCount;
        private CommittedRecord<int> totalOutputCount;
        private CommittedRecord<ImmutableSortedDictionary<UInt256, ChainedHeader>.Builder> headers;
        private CommittedRecord<ImmutableSortedDictionary<UInt256, UnspentTx>.Builder> unspentTransactions;
        private CommittedRecord<ImmutableSortedDictionary<TxOutputKey, TxOutput>.Builder> unspentTxOutputs;
        private CommittedRecord<ImmutableDictionary<int, BlockSpentTxes>.Builder> blockSpentTxes;
        private CommittedRecord<ImmutableDictionary<UInt256, IImmutableList<UnmintedTx>>.Builder> blockUnmintedTxes;

        public MemoryChainStateStorage(ChainedHeader chainTip = null, int? unspentTxCount = null, int? totalTxCount = null, int? totalInputCount = null, int? totalOutputCount = null, int? unspentOutputCount = null, ImmutableSortedDictionary<UInt256, ChainedHeader> headers = null, ImmutableSortedDictionary<UInt256, UnspentTx> unspentTransactions = null, ImmutableDictionary<int, BlockSpentTxes> blockSpentTxes = null, ImmutableDictionary<UInt256, IImmutableList<UnmintedTx>> blockUnmintedTxes = null)
        {
            this.chainTip = CommittedRecord<ChainedHeader>.Initial(chainTip);
            this.unspentTxCount = CommittedRecord<int>.Initial(unspentTxCount ?? 0);
            this.unspentOutputCount = CommittedRecord<int>.Initial(unspentOutputCount ?? 0);
            this.totalTxCount = CommittedRecord<int>.Initial(totalTxCount ?? 0);
            this.totalInputCount = CommittedRecord<int>.Initial(totalInputCount ?? 0);
            this.totalOutputCount = CommittedRecord<int>.Initial(totalOutputCount ?? 0);
            this.headers = CommittedRecord<ImmutableSortedDictionary<UInt256, ChainedHeader>.Builder>.Initial(
                headers?.ToBuilder() ?? ImmutableSortedDictionary.CreateBuilder<UInt256, ChainedHeader>());
            this.unspentTransactions = CommittedRecord<ImmutableSortedDictionary<UInt256, UnspentTx>.Builder>.Initial(
                unspentTransactions?.ToBuilder() ?? ImmutableSortedDictionary.CreateBuilder<UInt256, UnspentTx>());
            this.unspentTxOutputs = CommittedRecord<ImmutableSortedDictionary<TxOutputKey, TxOutput>.Builder>.Initial(
                ImmutableSortedDictionary.CreateBuilder<TxOutputKey, TxOutput>());
            this.blockSpentTxes = CommittedRecord<ImmutableDictionary<int, BlockSpentTxes>.Builder>.Initial(
                blockSpentTxes?.ToBuilder() ?? ImmutableDictionary.CreateBuilder<int, BlockSpentTxes>());
            this.blockUnmintedTxes = CommittedRecord<ImmutableDictionary<UInt256, IImmutableList<UnmintedTx>>.Builder>.Initial(
                blockUnmintedTxes?.ToBuilder() ?? ImmutableDictionary.CreateBuilder<UInt256, IImmutableList<UnmintedTx>>());
        }

        public void Dispose()
        {
            this.writeTxLock.Dispose();
        }

        internal SemaphoreSlim WriteTxLock => this.writeTxLock;

        public void BeginTransaction(
            out UncommittedRecord<ChainedHeader> chainTip,
            out UncommittedRecord<int> unspentTxCount,
            out UncommittedRecord<int> unspentOutputCount,
            out UncommittedRecord<int> totalTxCount,
            out UncommittedRecord<int> totalInputCount,
            out UncommittedRecord<int> totalOutputCount,
            out UncommittedRecord<ImmutableSortedDictionary<UInt256, ChainedHeader>.Builder> headers,
            out UncommittedRecord<ImmutableSortedDictionary<UInt256, UnspentTx>.Builder> unspentTransactions,
            out UncommittedRecord<ImmutableSortedDictionary<TxOutputKey, TxOutput>.Builder> unspentTxOutputs,
            out UncommittedRecord<ImmutableDictionary<int, BlockSpentTxes>.Builder> blockSpentTxes,
            out UncommittedRecord<ImmutableDictionary<UInt256, IImmutableList<UnmintedTx>>.Builder> blockUnmintedTxes)
        {
            lock (this.lockObject)
            {
                chainTip = this.chainTip.AsUncommitted();
                unspentTxCount = this.unspentTxCount.AsUncommitted();
                unspentOutputCount = this.unspentOutputCount.AsUncommitted();
                totalTxCount = this.totalTxCount.AsUncommitted();
                totalInputCount = this.totalInputCount.AsUncommitted();
                totalOutputCount = this.totalOutputCount.AsUncommitted();
                headers = this.headers.AsUncommitted(x => x.ToImmutable().ToBuilder());
                unspentTransactions = this.unspentTransactions.AsUncommitted(x => x.ToImmutable().ToBuilder());
                unspentTxOutputs = this.unspentTxOutputs.AsUncommitted(x => x.ToImmutable().ToBuilder());
                blockSpentTxes = this.blockSpentTxes.AsUncommitted(x => x.ToImmutable().ToBuilder());
                blockUnmintedTxes = this.blockUnmintedTxes.AsUncommitted(x => x.ToImmutable().ToBuilder());
            }
        }

        public void CommitTransaction(
            UncommittedRecord<ChainedHeader> chainTip,
            UncommittedRecord<int> unspentTxCount,
            UncommittedRecord<int> unspentOutputCount,
            UncommittedRecord<int> totalTxCount,
            UncommittedRecord<int> totalInputCount,
            UncommittedRecord<int> totalOutputCount,
            UncommittedRecord<ImmutableSortedDictionary<UInt256, ChainedHeader>.Builder> headers,
            UncommittedRecord<ImmutableSortedDictionary<UInt256, UnspentTx>.Builder> unspentTransactions,
            UncommittedRecord<ImmutableSortedDictionary<TxOutputKey, TxOutput>.Builder> unspentTxOutputs,
            UncommittedRecord<ImmutableDictionary<int, BlockSpentTxes>.Builder> blockSpentTxes,
            UncommittedRecord<ImmutableDictionary<UInt256, IImmutableList<UnmintedTx>>.Builder> blockUnmintedTxes)
        {
            lock (this.lockObject)
            {
                if (this.chainTip.ConflictsWith(chainTip)
                    || this.unspentTxCount.ConflictsWith(unspentTxCount)
                    || this.unspentOutputCount.ConflictsWith(unspentOutputCount)
                    || this.totalTxCount.ConflictsWith(totalTxCount)
                    || this.totalInputCount.ConflictsWith(totalInputCount)
                    || this.totalOutputCount.ConflictsWith(totalOutputCount)
                    || this.headers.ConflictsWith(headers)
                    || this.unspentTransactions.ConflictsWith(unspentTransactions)
                    || this.unspentTxOutputs.ConflictsWith(unspentTxOutputs)
                    || this.blockSpentTxes.ConflictsWith(blockSpentTxes)
                    || this.blockUnmintedTxes.ConflictsWith(blockUnmintedTxes))
                    throw new InvalidOperationException();

                this.chainTip.Committ(chainTip);
                this.unspentTxCount.Committ(unspentTxCount);
                this.unspentOutputCount.Committ(unspentOutputCount);
                this.totalTxCount.Committ(totalTxCount);
                this.totalInputCount.Committ(totalInputCount);
                this.totalOutputCount.Committ(totalOutputCount);
                this.headers.Committ(headers, x => x.ToImmutable().ToBuilder());
                this.unspentTransactions.Committ(unspentTransactions, x => x.ToImmutable().ToBuilder());
                this.unspentTxOutputs.Committ(unspentTxOutputs, x => x.ToImmutable().ToBuilder());
                this.blockSpentTxes.Committ(blockSpentTxes, x => x.ToImmutable().ToBuilder());
                this.blockUnmintedTxes.Committ(blockUnmintedTxes, x => x.ToImmutable().ToBuilder());
            }
        }
    }
}
