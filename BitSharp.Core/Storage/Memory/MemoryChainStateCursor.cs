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
        private UncommittedRecord<ImmutableSortedDictionary<TxOutputKey, TxOutput>.Builder> unspentTxOutputs;
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
                chainStateStorage.WriteTxLock.Wait();

            chainStateStorage.BeginTransaction(
                out chainTip,
                out unspentTxCount,
                out unspentOutputCount,
                out totalTxCount,
                out totalInputCount,
                out totalOutputCount,
                out headers,
                out unspentTransactions,
                out unspentTxOutputs,
                out blockSpentTxes,
                out blockUnmintedTxes);

            inTransaction = true;
        }

        public void CommitTransaction()
        {
            if (!inTransaction)
                throw new InvalidOperationException();

            chainStateStorage.CommitTransaction(
                chainTip,
                unspentTxCount,
                unspentOutputCount,
                totalTxCount,
                totalInputCount,
                totalOutputCount,
                headers,
                unspentTransactions,
                unspentTxOutputs,
                blockSpentTxes,
                blockUnmintedTxes);

            chainTip = null;

            unspentTxCount = null;
            unspentOutputCount = null;
            totalTxCount = null;
            totalInputCount = null;
            totalOutputCount = null;
            headers = null;
            unspentTransactions = null;
            unspentTxOutputs = null;
            blockSpentTxes = null;
            blockUnmintedTxes = null;

            inTransaction = false;

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
            if (!inTransaction)
                throw new InvalidOperationException();

            chainTip = null;
            unspentTxCount = null;
            unspentOutputCount = null;
            totalTxCount = null;
            totalInputCount = null;
            totalOutputCount = null;
            headers = null;
            unspentTransactions = null;
            unspentTxOutputs = null;
            blockSpentTxes = null;
            blockUnmintedTxes = null;

            inTransaction = false;

            if (!readOnly)
                chainStateStorage.WriteTxLock.Release();
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

        public int UnspentTxCount
        {
            get
            {
                CheckTransaction();
                return unspentTxCount.Value;
            }
            set
            {
                CheckWriteTransaction();
                unspentTxCount.Value = value;
            }
        }

        public int UnspentOutputCount
        {
            get
            {
                CheckTransaction();
                return unspentOutputCount.Value;
            }
            set
            {
                CheckWriteTransaction();
                unspentOutputCount.Value = value;
            }
        }

        public int TotalTxCount
        {
            get
            {
                CheckTransaction();
                return totalTxCount.Value;
            }
            set
            {
                CheckWriteTransaction();
                totalTxCount.Value = value;
            }
        }

        public int TotalInputCount
        {
            get
            {
                CheckTransaction();
                return totalInputCount.Value;
            }
            set
            {
                CheckWriteTransaction();
                totalInputCount.Value = value;
            }
        }

        public int TotalOutputCount
        {
            get
            {
                CheckTransaction();
                return totalOutputCount.Value;
            }
            set
            {
                CheckWriteTransaction();
                totalOutputCount.Value = value;
            }
        }

        public bool ContainsHeader(UInt256 blockHash)
        {
            CheckTransaction();
            return headers.Value.ContainsKey(blockHash);
        }

        public bool TryGetHeader(UInt256 blockHash, out ChainedHeader header)
        {
            CheckTransaction();
            return headers.Value.TryGetValue(blockHash, out header);
        }

        public bool TryAddHeader(ChainedHeader header)
        {
            CheckWriteTransaction();

            try
            {
                headers.Modify(x => x.Add(header.Hash, header));
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
            return headers.TryModify(x => x.Remove(blockHash));
        }

        public bool ContainsUnspentTx(UInt256 txHash)
        {
            CheckTransaction();
            return unspentTransactions.Value.ContainsKey(txHash);
        }

        public bool TryGetUnspentTx(UInt256 txHash, out UnspentTx unspentTx)
        {
            CheckTransaction();
            return unspentTransactions.Value.TryGetValue(txHash, out unspentTx);
        }

        public bool TryAddUnspentTx(UnspentTx unspentTx)
        {
            CheckWriteTransaction();

            try
            {
                unspentTransactions.Modify(x => x.Add(unspentTx.TxHash, unspentTx));
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
            return unspentTransactions.TryModify(x => x.Remove(txHash));
        }

        public void RemoveUnspentTx(UInt256 txHash)
        {
            TryRemoveUnspentTx(txHash);
        }

        public bool TryUpdateUnspentTx(UnspentTx unspentTx)
        {
            CheckWriteTransaction();

            if (unspentTransactions.Value.ContainsKey(unspentTx.TxHash))
            {
                unspentTransactions.Modify(x => x[unspentTx.TxHash] = unspentTx);
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
            return unspentTransactions.Value.Values;
        }

        public bool ContainsUnspentTxOutput(TxOutputKey txOutputKey)
        {
            CheckTransaction();
            return unspentTxOutputs.Value.ContainsKey(txOutputKey);
        }

        public bool TryGetUnspentTxOutput(TxOutputKey txOutputKey, out TxOutput txOutput)
        {
            CheckTransaction();
            return unspentTxOutputs.Value.TryGetValue(txOutputKey, out txOutput);
        }

        public bool TryAddUnspentTxOutput(TxOutputKey txOutputKey, TxOutput txOutput)
        {
            CheckWriteTransaction();

            try
            {
                unspentTxOutputs.Modify(x => x.Add(txOutputKey, txOutput));
                return true;
            }
            catch (ArgumentException)
            {
                return false;
            }
        }

        public bool TryRemoveUnspentTxOutput(TxOutputKey txOutputKey)
        {
            CheckWriteTransaction();
            return unspentTxOutputs.TryModify(x => x.Remove(txOutputKey));
        }

        public void RemoveUnspentTxOutput(TxOutputKey txOutputKey)
        {
            TryRemoveUnspentTxOutput(txOutputKey);
        }

        public bool ContainsBlockSpentTxes(int blockIndex)
        {
            CheckTransaction();
            return blockSpentTxes.Value.ContainsKey(blockIndex);
        }

        public bool TryGetBlockSpentTxes(int blockIndex, out BlockSpentTxes spentTxes)
        {
            CheckTransaction();
            return blockSpentTxes.Value.TryGetValue(blockIndex, out spentTxes);
        }

        public bool TryAddBlockSpentTxes(int blockIndex, BlockSpentTxes spentTxes)
        {
            CheckWriteTransaction();

            try
            {
                blockSpentTxes.Modify(x => x.Add(blockIndex, spentTxes));
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
            return blockSpentTxes.TryModify(x => x.Remove(blockIndex));
        }

        public bool ContainsBlockUnmintedTxes(UInt256 blockHash)
        {
            CheckTransaction();
            return blockUnmintedTxes.Value.ContainsKey(blockHash);
        }

        public bool TryGetBlockUnmintedTxes(UInt256 blockHash, out IImmutableList<UnmintedTx> unmintedTxes)
        {
            CheckTransaction();
            return blockUnmintedTxes.Value.TryGetValue(blockHash, out unmintedTxes);
        }

        public bool TryAddBlockUnmintedTxes(UInt256 blockHash, IImmutableList<UnmintedTx> unmintedTxes)
        {
            CheckWriteTransaction();

            try
            {
                blockUnmintedTxes.Modify(x => x.Add(blockHash, unmintedTxes));
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
            return blockUnmintedTxes.TryModify(x => x.Remove(blockHash));
        }

        public void Flush()
        {
        }

        public void Defragment()
        {
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
