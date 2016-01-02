using BitSharp.Common;
using BitSharp.Core.Domain;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace BitSharp.Core.Storage.Memory
{
    public class MemoryUnconfirmedTxesCursor : IUnconfirmedTxesCursor
    {
        private readonly MemoryUnconfirmedTxesStorage unconfirmedTxesStorage;

        private bool inTransaction;
        private bool readOnly;

        private UncommittedRecord<ChainedHeader> chainTip;
        private UncommittedRecord<int> unconfirmedTxCount;
        private UncommittedRecord<ImmutableDictionary<UInt256, UnconfirmedTx>.Builder> unconfirmedTxes;

        private bool isDisposed;

        internal MemoryUnconfirmedTxesCursor(MemoryUnconfirmedTxesStorage unconfirmedTxesStorage)
        {
            this.unconfirmedTxesStorage = unconfirmedTxesStorage;
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
                if (this.inTransaction)
                    this.RollbackTransaction();

                isDisposed = true;
            }
        }

        public bool InTransaction => this.inTransaction;

        public void BeginTransaction(bool readOnly, bool pruneOnly)
        {
            if (this.inTransaction)
                throw new InvalidOperationException();

            this.readOnly = readOnly;
            if (!readOnly)
                unconfirmedTxesStorage.WriteTxLock.Wait();

            this.unconfirmedTxesStorage.BeginTransaction(
                out this.chainTip,
                out this.unconfirmedTxCount,
                out this.unconfirmedTxes);

            this.inTransaction = true;
        }

        public void CommitTransaction()
        {
            if (!this.inTransaction)
                throw new InvalidOperationException();

            this.unconfirmedTxesStorage.CommitTransaction(
                this.chainTip,
                this.unconfirmedTxCount,
                this.unconfirmedTxes);

            this.chainTip = null;
            this.unconfirmedTxCount = null;
            this.unconfirmedTxes = null;

            this.inTransaction = false;

            if (!readOnly)
                unconfirmedTxesStorage.WriteTxLock.Release();
        }

        public void RollbackTransaction()
        {
            if (!this.inTransaction)
                throw new InvalidOperationException();

            this.chainTip = null;
            this.unconfirmedTxCount = null;
            this.unconfirmedTxes = null;

            this.inTransaction = false;

            if (!readOnly)
                unconfirmedTxesStorage.WriteTxLock.Release();
        }

        public ChainedHeader ChainTip
        {
            get
            {
                CheckTransaction();
                return this.chainTip.Value;
            }
            set
            {
                CheckWriteTransaction();
                this.chainTip.Value = value;
            }
        }

        public int UnconfirmedTxCount
        {
            get
            {
                CheckTransaction();
                return this.unconfirmedTxCount.Value;
            }
            set
            {
                CheckWriteTransaction();
                this.unconfirmedTxCount.Value = value;
            }
        }

        public bool ContainsTransaction(UInt256 txHash)
        {
            CheckTransaction();
            return this.unconfirmedTxes.Value.ContainsKey(txHash);
        }

        public bool TryGetTransaction(UInt256 txHash, out UnconfirmedTx unconfimedTx)
        {
            CheckTransaction();
            return this.unconfirmedTxes.Value.TryGetValue(txHash, out unconfimedTx);
        }

        public bool TryAddTransaction(UnconfirmedTx unconfirmedTx)
        {
            CheckWriteTransaction();

            try
            {
                this.unconfirmedTxes.Modify(x => x.Add(unconfirmedTx.Hash, unconfirmedTx));
                return true;
            }
            catch (ArgumentException)
            {
                return false;
            }
        }

        public bool TryRemoveTransaction(UInt256 txHash)
        {
            CheckWriteTransaction();
            return this.unconfirmedTxes.TryModify(x => x.Remove(txHash));
        }

        private void CheckTransaction()
        {
            if (!this.inTransaction)
                throw new InvalidOperationException();
        }

        private void CheckWriteTransaction()
        {
            if (!this.inTransaction || this.readOnly)
                throw new InvalidOperationException();
        }
    }
}
