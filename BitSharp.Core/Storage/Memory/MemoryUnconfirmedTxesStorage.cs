using BitSharp.Common;
using BitSharp.Core.Domain;
using System;
using System.Collections.Immutable;
using System.Threading;

namespace BitSharp.Core.Storage.Memory
{
    internal class MemoryUnconfirmedTxesStorage : IDisposable
    {
        private readonly object lockObject = new object();
        private readonly SemaphoreSlim writeTxLock = new SemaphoreSlim(1);

        private CommittedRecord<ChainedHeader> chainTip;
        private CommittedRecord<int> unconfirmedTxCount;
        private CommittedRecord<ImmutableDictionary<UInt256, UnconfirmedTx>.Builder> unconfirmedTxes;
        private CommittedRecord<ImmutableDictionary<TxOutputKey, ImmutableDictionary<UInt256, UnconfirmedTx>.Builder>.Builder> unconfirmedTxesByPrevTxOutputKey;

        public MemoryUnconfirmedTxesStorage()
        {
            this.chainTip = CommittedRecord<ChainedHeader>.Initial(null);
            this.unconfirmedTxCount = CommittedRecord<int>.Initial(0);
            this.unconfirmedTxes = CommittedRecord<ImmutableDictionary<UInt256, UnconfirmedTx>.Builder>.Initial(
                ImmutableDictionary.CreateBuilder<UInt256, UnconfirmedTx>());
            this.unconfirmedTxesByPrevTxOutputKey = CommittedRecord<ImmutableDictionary<TxOutputKey, ImmutableDictionary<UInt256, UnconfirmedTx>.Builder>.Builder>.Initial(
                ImmutableDictionary.CreateBuilder<TxOutputKey, ImmutableDictionary<UInt256, UnconfirmedTx>.Builder>());
        }

        public void Dispose()
        {
            this.writeTxLock.Dispose();
        }

        internal SemaphoreSlim WriteTxLock => this.writeTxLock;

        public void BeginTransaction(
            out UncommittedRecord<ChainedHeader> chainTip,
            out UncommittedRecord<int> unconfirmedTxCount,
            out UncommittedRecord<ImmutableDictionary<UInt256, UnconfirmedTx>.Builder> unconfirmedTxes,
            out UncommittedRecord<ImmutableDictionary<TxOutputKey, ImmutableDictionary<UInt256, UnconfirmedTx>.Builder>.Builder> unconfirmedTxesByPrevTxOutputKey)
        {
            lock (this.lockObject)
            {
                chainTip = this.chainTip.AsUncommitted();
                unconfirmedTxCount = this.unconfirmedTxCount.AsUncommitted();
                unconfirmedTxes = this.unconfirmedTxes.AsUncommitted(x => x.ToImmutable().ToBuilder());
                unconfirmedTxesByPrevTxOutputKey = this.unconfirmedTxesByPrevTxOutputKey.AsUncommitted(x => x.ToImmutable().ToBuilder());
            }
        }

        public void CommitTransaction(
            UncommittedRecord<ChainedHeader> chainTip,
            UncommittedRecord<int> unconfirmedTxCount,
            UncommittedRecord<ImmutableDictionary<UInt256, UnconfirmedTx>.Builder> unconfirmedTxes,
            UncommittedRecord<ImmutableDictionary<TxOutputKey, ImmutableDictionary<UInt256, UnconfirmedTx>.Builder>.Builder> unconfirmedTxesByPrevTxOutputKey)
        {
            lock (this.lockObject)
            {
                if (this.chainTip.ConflictsWith(chainTip)
                    || this.unconfirmedTxCount.ConflictsWith(unconfirmedTxCount)
                    || this.unconfirmedTxes.ConflictsWith(unconfirmedTxes)
                    || this.unconfirmedTxesByPrevTxOutputKey.ConflictsWith(unconfirmedTxesByPrevTxOutputKey))
                    throw new InvalidOperationException();

                this.chainTip.Committ(chainTip);
                this.unconfirmedTxCount.Committ(unconfirmedTxCount);
                this.unconfirmedTxes.Committ(unconfirmedTxes, x => x.ToImmutable().ToBuilder());
                this.unconfirmedTxesByPrevTxOutputKey.Committ(unconfirmedTxesByPrevTxOutputKey, x => x.ToImmutable().ToBuilder());
            }
        }
    }
}
