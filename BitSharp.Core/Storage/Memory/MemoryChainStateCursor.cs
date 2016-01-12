using BitSharp.Common;
using BitSharp.Core.Domain;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading.Tasks;

namespace BitSharp.Core.Storage.Memory
{
    public class MemoryChainStateCursor : IChainStateCursor
    {
        private readonly MemoryChainStateStorage chainStateStorage;

        private bool inTransaction;
        private bool readOnly;

        private UncommittedRecord<ChainedHeader> chainTip;
        private UncommittedRecord<int> unspentTxCount;
        private UncommittedRecord<int> unspentOutputCount;
        private UncommittedRecord<int> totalTxCount;
        private UncommittedRecord<int> totalInputCount;
        private UncommittedRecord<int> totalOutputCount;
        private UncommittedRecord<ImmutableSortedDictionary<UInt256, ChainedHeader>.Builder> headers;
        private UncommittedRecord<ImmutableSortedDictionary<UInt256, UnspentTx>.Builder> unspentTransactions;
        private UncommittedRecord<ImmutableDictionary<int, BlockSpentTxes>.Builder> blockSpentTxes;
        private UncommittedRecord<ImmutableDictionary<UInt256, IImmutableList<UnmintedTx>>.Builder> blockUnmintedTxes;

        private bool isDisposed;

        internal MemoryChainStateCursor(MemoryChainStateStorage chainStateStorage)
        {
            this.chainStateStorage = chainStateStorage;
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
                chainStateStorage.WriteTxLock.Wait();

            this.chainStateStorage.BeginTransaction(
                out this.chainTip,
                out this.unspentTxCount,
                out this.unspentOutputCount,
                out this.totalTxCount,
                out this.totalInputCount,
                out this.totalOutputCount,
                out this.headers,
                out this.unspentTransactions,
                out this.blockSpentTxes,
                out this.blockUnmintedTxes);

            this.inTransaction = true;
        }

        public void CommitTransaction()
        {
            if (!this.inTransaction)
                throw new InvalidOperationException();

            this.chainStateStorage.CommitTransaction(
                this.chainTip,
                this.unspentTxCount,
                this.unspentOutputCount,
                this.totalTxCount,
                this.totalInputCount,
                this.totalOutputCount,
                this.headers,
                this.unspentTransactions,
                this.blockSpentTxes,
                this.blockUnmintedTxes);

            this.chainTip = null;

            this.unspentTxCount = null;
            this.unspentOutputCount = null;
            this.totalTxCount = null;
            this.totalInputCount = null;
            this.totalOutputCount = null;
            this.headers = null;
            this.unspentTransactions = null;
            this.blockSpentTxes = null;
            this.blockUnmintedTxes = null;

            this.inTransaction = false;

            if (!readOnly)
                chainStateStorage.WriteTxLock.Release();
        }

        public Task CommitTransactionAsync()
        {
            CommitTransaction();
            return Task.CompletedTask;
        }

        public void RollbackTransaction()
        {
            if (!this.inTransaction)
                throw new InvalidOperationException();

            this.chainTip = null;
            this.unspentTxCount = null;
            this.unspentOutputCount = null;
            this.totalTxCount = null;
            this.totalInputCount = null;
            this.totalOutputCount = null;
            this.headers = null;
            this.unspentTransactions = null;
            this.blockSpentTxes = null;
            this.blockUnmintedTxes = null;

            this.inTransaction = false;

            if (!readOnly)
                chainStateStorage.WriteTxLock.Release();
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

        public int UnspentTxCount
        {
            get
            {
                CheckTransaction();
                return this.unspentTxCount.Value;
            }
            set
            {
                CheckWriteTransaction();
                this.unspentTxCount.Value = value;
            }
        }

        public int UnspentOutputCount
        {
            get
            {
                CheckTransaction();
                return this.unspentOutputCount.Value;
            }
            set
            {
                CheckWriteTransaction();
                this.unspentOutputCount.Value = value;
            }
        }

        public int TotalTxCount
        {
            get
            {
                CheckTransaction();
                return this.totalTxCount.Value;
            }
            set
            {
                CheckWriteTransaction();
                this.totalTxCount.Value = value;
            }
        }

        public int TotalInputCount
        {
            get
            {
                CheckTransaction();
                return this.totalInputCount.Value;
            }
            set
            {
                CheckWriteTransaction();
                this.totalInputCount.Value = value;
            }
        }

        public int TotalOutputCount
        {
            get
            {
                CheckTransaction();
                return this.totalOutputCount.Value;
            }
            set
            {
                CheckWriteTransaction();
                this.totalOutputCount.Value = value;
            }
        }

        public bool ContainsHeader(UInt256 blockHash)
        {
            CheckTransaction();
            return this.headers.Value.ContainsKey(blockHash);
        }

        public bool TryGetHeader(UInt256 blockHash, out ChainedHeader header)
        {
            CheckTransaction();
            return this.headers.Value.TryGetValue(blockHash, out header);
        }

        public bool TryAddHeader(ChainedHeader header)
        {
            CheckWriteTransaction();

            try
            {
                this.headers.Modify(x => x.Add(header.Hash, header));
                return true;
            }
            catch (ArgumentException)
            {
                return false;
            }
        }

        public bool TryRemoveHeader(UInt256 blockHash)
        {
            CheckWriteTransaction();
            return this.headers.TryModify(x => x.Remove(blockHash));
        }

        public bool ContainsUnspentTx(UInt256 txHash)
        {
            CheckTransaction();
            return this.unspentTransactions.Value.ContainsKey(txHash);
        }

        public bool TryGetUnspentTx(UInt256 txHash, out UnspentTx unspentTx)
        {
            CheckTransaction();
            return this.unspentTransactions.Value.TryGetValue(txHash, out unspentTx);
        }

        public bool TryAddUnspentTx(UnspentTx unspentTx)
        {
            CheckWriteTransaction();

            try
            {
                this.unspentTransactions.Modify(x => x.Add(unspentTx.TxHash, unspentTx));
                return true;
            }
            catch (ArgumentException)
            {
                return false;
            }
        }

        public bool TryRemoveUnspentTx(UInt256 txHash)
        {
            CheckWriteTransaction();
            return this.unspentTransactions.TryModify(x => x.Remove(txHash));
        }

        public void RemoveUnspentTx(UInt256 txHash)
        {
            TryRemoveUnspentTx(txHash);
        }

        public bool TryUpdateUnspentTx(UnspentTx unspentTx)
        {
            CheckWriteTransaction();

            if (this.unspentTransactions.Value.ContainsKey(unspentTx.TxHash))
            {
                this.unspentTransactions.Modify(x => x[unspentTx.TxHash] = unspentTx);
                return true;
            }
            else
            {
                return false;
            }
        }

        public IEnumerable<UnspentTx> ReadUnspentTransactions()
        {
            CheckTransaction();
            return this.unspentTransactions.Value.Values;
        }

        public bool ContainsBlockSpentTxes(int blockIndex)
        {
            CheckTransaction();
            return this.blockSpentTxes.Value.ContainsKey(blockIndex);
        }

        public bool TryGetBlockSpentTxes(int blockIndex, out BlockSpentTxes spentTxes)
        {
            CheckTransaction();
            return this.blockSpentTxes.Value.TryGetValue(blockIndex, out spentTxes);
        }

        public bool TryAddBlockSpentTxes(int blockIndex, BlockSpentTxes spentTxes)
        {
            CheckWriteTransaction();

            try
            {
                this.blockSpentTxes.Modify(x => x.Add(blockIndex, spentTxes));
                return true;
            }
            catch (ArgumentException)
            {
                return false;
            }
        }

        public bool TryRemoveBlockSpentTxes(int blockIndex)
        {
            CheckWriteTransaction();
            return this.blockSpentTxes.TryModify(x => x.Remove(blockIndex));
        }

        public bool ContainsBlockUnmintedTxes(UInt256 blockHash)
        {
            CheckTransaction();
            return this.blockUnmintedTxes.Value.ContainsKey(blockHash);
        }

        public bool TryGetBlockUnmintedTxes(UInt256 blockHash, out IImmutableList<UnmintedTx> unmintedTxes)
        {
            CheckTransaction();
            return this.blockUnmintedTxes.Value.TryGetValue(blockHash, out unmintedTxes);
        }

        public bool TryAddBlockUnmintedTxes(UInt256 blockHash, IImmutableList<UnmintedTx> unmintedTxes)
        {
            CheckWriteTransaction();

            try
            {
                this.blockUnmintedTxes.Modify(x => x.Add(blockHash, unmintedTxes));
                return true;
            }
            catch (ArgumentException)
            {
                return false;
            }
        }

        public bool TryRemoveBlockUnmintedTxes(UInt256 blockHash)
        {
            CheckWriteTransaction();
            return this.blockUnmintedTxes.TryModify(x => x.Remove(blockHash));
        }

        public void Flush()
        {
        }

        public void Defragment()
        {
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

        public bool ContainsUnspentTxOutput(TxOutputKey txOutputKey)
        {
            throw new NotImplementedException();
        }

        public bool TryGetUnspentTxOutput(TxOutputKey txOutputKey, out TxOutput txOutput)
        {
            throw new NotImplementedException();
        }

        public bool TryAddUnspentTxOutput(TxOutputKey txOutputKey, TxOutput txOutput)
        {
            throw new NotImplementedException();
        }

        public bool TryRemoveUnspentTxOutput(TxOutputKey txOutputKey)
        {
            throw new NotImplementedException();
        }

        public void RemoveUnspentTxOutput(TxOutputKey txOutputKey)
        {
            throw new NotImplementedException();
        }
    }
}
