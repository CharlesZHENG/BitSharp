using BitSharp.Common;
using BitSharp.Core.Domain;
using System;
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
        private UncommittedRecord<ImmutableDictionary<TxOutputKey, ImmutableDictionary<UInt256, UnconfirmedTx>.Builder>.Builder> unconfirmedTxesByPrevTxOutputKey;

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
                if (inTransaction)
                    RollbackTransaction();

                isDisposed = true;
            }
        }

        public bool InTransaction => inTransaction;

        public void BeginTransaction(bool readOnly)
        {
            if (inTransaction)
                throw new InvalidOperationException();

            this.readOnly = readOnly;
            if (!readOnly)
                unconfirmedTxesStorage.WriteTxLock.Wait();

            unconfirmedTxesStorage.BeginTransaction(
                out chainTip,
                out unconfirmedTxCount,
                out unconfirmedTxes,
                out unconfirmedTxesByPrevTxOutputKey);

            inTransaction = true;
        }

        public void CommitTransaction()
        {
            if (!inTransaction)
                throw new InvalidOperationException();

            unconfirmedTxesStorage.CommitTransaction(
                chainTip,
                unconfirmedTxCount,
                unconfirmedTxes,
                unconfirmedTxesByPrevTxOutputKey);

            chainTip = null;
            unconfirmedTxCount = null;
            unconfirmedTxes = null;
            unconfirmedTxesByPrevTxOutputKey = null;

            inTransaction = false;

            if (!readOnly)
                unconfirmedTxesStorage.WriteTxLock.Release();
        }

        public void RollbackTransaction()
        {
            if (!inTransaction)
                throw new InvalidOperationException();

            chainTip = null;
            unconfirmedTxCount = null;
            unconfirmedTxes = null;
            unconfirmedTxesByPrevTxOutputKey = null;

            inTransaction = false;

            if (!readOnly)
                unconfirmedTxesStorage.WriteTxLock.Release();
        }

        public ChainedHeader ChainTip
        {
            get
            {
                CheckTransaction();
                return chainTip.Value;
            }
            set
            {
                CheckWriteTransaction();
                chainTip.Value = value;
            }
        }

        public int UnconfirmedTxCount
        {
            get
            {
                CheckTransaction();
                return unconfirmedTxCount.Value;
            }
            set
            {
                CheckWriteTransaction();
                unconfirmedTxCount.Value = value;
            }
        }

        public bool ContainsTransaction(UInt256 txHash)
        {
            CheckTransaction();
            return unconfirmedTxes.Value.ContainsKey(txHash);
        }

        public bool TryGetTransaction(UInt256 txHash, out UnconfirmedTx unconfimedTx)
        {
            CheckTransaction();
            return unconfirmedTxes.Value.TryGetValue(txHash, out unconfimedTx);
        }

        public bool TryAddTransaction(UnconfirmedTx unconfirmedTx)
        {
            CheckWriteTransaction();

            try
            {
                unconfirmedTxes.Modify(x => x.Add(unconfirmedTx.Hash, unconfirmedTx));

                // update index of txes spending each input's prev tx
                for (var inputIndex = 0; inputIndex < unconfirmedTx.Transaction.Inputs.Length; inputIndex++)
                {
                    var input = unconfirmedTx.Transaction.Inputs[inputIndex];

                    unconfirmedTxesByPrevTxOutputKey.Modify(_ => { });
                    ImmutableDictionary<UInt256, UnconfirmedTx>.Builder unconfirmedTxes;
                    if (unconfirmedTxesByPrevTxOutputKey.Value.TryGetValue(input.PrevTxOutputKey, out unconfirmedTxes))
                        // ensure a copy of the builder is modified or underlying storage will see uncomitted state
                        unconfirmedTxes = unconfirmedTxes.ToImmutable().ToBuilder();
                    else
                        unconfirmedTxes = ImmutableDictionary.CreateBuilder<UInt256, UnconfirmedTx>();

                    unconfirmedTxes.Add(unconfirmedTx.Hash, unconfirmedTx);
                    unconfirmedTxesByPrevTxOutputKey.Value[input.PrevTxOutputKey] = unconfirmedTxes;
                }

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

            UnconfirmedTx unconfirmedTx;
            if (unconfirmedTxes.Value.TryGetValue(txHash, out unconfirmedTx)
                && unconfirmedTxes.TryModify(x => x.Remove(txHash)))
            {
                // update index of txes spending each input's prev tx
                for (var inputIndex = 0; inputIndex < unconfirmedTx.Transaction.Inputs.Length; inputIndex++)
                {
                    var input = unconfirmedTx.Transaction.Inputs[inputIndex];

                    unconfirmedTxesByPrevTxOutputKey.Modify(_ => { });
                    ImmutableDictionary<UInt256, UnconfirmedTx>.Builder unconfirmedTxes;
                    if (unconfirmedTxesByPrevTxOutputKey.Value.TryGetValue(input.PrevTxOutputKey, out unconfirmedTxes))
                        // ensure a copy of the builder is modified or underlying storage will see uncomitted state
                        unconfirmedTxes = unconfirmedTxes.ToImmutable().ToBuilder();
                    else
                        unconfirmedTxes = ImmutableDictionary.CreateBuilder<UInt256, UnconfirmedTx>();

                    unconfirmedTxes.Remove(unconfirmedTx.Hash);
                    if (unconfirmedTxes.Count > 0)
                        unconfirmedTxesByPrevTxOutputKey.Value[input.PrevTxOutputKey] = unconfirmedTxes;
                    else
                        unconfirmedTxesByPrevTxOutputKey.Value.Remove(input.PrevTxOutputKey);
                }

                return true;
            }
            else
                return false;
        }

        public ImmutableDictionary<UInt256, UnconfirmedTx> GetTransactionsSpending(TxOutputKey prevTxOutputKey)
        {
            ImmutableDictionary<UInt256, UnconfirmedTx>.Builder unconfirmedTxes;
            if (unconfirmedTxesByPrevTxOutputKey.Value.TryGetValue(prevTxOutputKey, out unconfirmedTxes))
                return unconfirmedTxes.ToImmutable();
            else
                return ImmutableDictionary<UInt256, UnconfirmedTx>.Empty;
        }

        private void CheckTransaction()
        {
            if (!inTransaction)
                throw new InvalidOperationException();
        }

        private void CheckWriteTransaction()
        {
            if (!inTransaction || readOnly)
                throw new InvalidOperationException();
        }
    }
}
