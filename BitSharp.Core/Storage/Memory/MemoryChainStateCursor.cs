using BitSharp.Common;
using BitSharp.Core.Domain;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace BitSharp.Core.Storage.Memory
{
    public class MemoryChainStateCursor : IChainStateCursor
    {
        private readonly MemoryChainStateStorage chainStateStorage;

        private bool inTransaction;
        private bool readOnly;

        private ChainedHeader chainTip;
        private int? unspentTxCount;
        private int? unspentOutputCount;
        private int? totalTxCount;
        private int? totalInputCount;
        private int? totalOutputCount;
        private ImmutableSortedDictionary<UInt256, ChainedHeader>.Builder headers;
        private ImmutableSortedDictionary<UInt256, UnspentTx>.Builder unspentTransactions;
        private ImmutableDictionary<int, IImmutableList<UInt256>>.Builder blockSpentTxes;
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

        private bool chainTipModified;
        private bool unspentTxCountModified;
        private bool unspentOutputCountModified;
        private bool totalTxCountModified;
        private bool totalInputCountModified;
        private bool totalOutputCountModified;
        private bool headersModified;
        private bool unspentTxesModified;
        private bool blockSpentTxesModified;
        private bool blockUnmintedTxesModified;

        private bool isDisposed;

        internal MemoryChainStateCursor(MemoryChainStateStorage chainStateStorage)
        {
            this.chainStateStorage = chainStateStorage;
        }

        internal ImmutableSortedDictionary<UInt256, UnspentTx>.Builder UnspentTransactionsDictionary { get { return this.unspentTransactions; } }

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

        public bool InTransaction
        {
            get { return this.inTransaction; }
        }

        public void BeginTransaction(bool readOnly)
        {
            if (this.inTransaction)
                throw new InvalidOperationException();

            this.readOnly = readOnly;
            if (!readOnly)
                chainStateStorage.WriteTxLock.Wait();

            this.chainStateStorage.BeginTransaction(out this.chainTip, out this.unspentTxCount, out this.unspentOutputCount, out this.totalTxCount, out this.totalInputCount, out this.totalOutputCount, out this.headers, out this.unspentTransactions, out this.blockSpentTxes, out this.blockUnmintedTxes, out this.chainTipVersion, out this.unspentTxCountVersion, out this.unspentOutputCountVersion, out this.totalTxCountVersion, out this.totalInputCountVersion, out this.totalOutputCountVersion, out this.headersVersion, out this.unspentTxesVersion, out this.blockSpentTxesVersion, out this.blockUnmintedTxesVersion);

            this.chainTipModified = false;
            this.unspentTxCountModified = false;
            this.unspentOutputCountModified = false;
            this.totalTxCountModified = false;
            this.totalInputCountModified = false;
            this.totalOutputCountModified = false;
            this.unspentTxesModified = false;
            this.blockSpentTxesModified = false;
            this.blockUnmintedTxesModified = false;

            this.inTransaction = true;
        }

        public void CommitTransaction()
        {
            if (!this.inTransaction)
                throw new InvalidOperationException();

            this.chainStateStorage.CommitTransaction(
                this.chainTipModified ? this.chainTip : null,
                this.unspentTxCountModified ? this.unspentTxCount : null,
                this.unspentOutputCountModified ? this.unspentOutputCount : null,
                this.totalTxCountModified ? this.totalTxCount : null,
                this.totalInputCountModified ? this.totalInputCount : null,
                this.totalOutputCountModified ? this.totalOutputCount : null,
                this.headersModified ? this.headers : null,
                this.unspentTxesModified ? this.unspentTransactions : null,
                this.blockSpentTxesModified ? this.blockSpentTxes : null,
                this.blockUnmintedTxesModified ? this.blockUnmintedTxes : null,
                this.chainTipVersion, this.unspentTxCountVersion, this.unspentOutputCountVersion, this.totalTxCountVersion, this.totalInputCountVersion, this.totalOutputCountVersion, this.headersVersion, this.unspentTxesVersion, this.blockSpentTxesVersion, this.blockUnmintedTxesVersion);

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
                return this.chainTip;
            }
            set
            {
                CheckWriteTransaction();

                this.chainTip = value;
                this.chainTipModified = true;
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

                this.unspentTxCount = value;
                this.unspentTxCountModified = true;
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

                this.unspentOutputCount = value;
                this.unspentOutputCountModified = true;
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

                this.totalTxCount = value;
                this.totalTxCountModified = true;
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

                this.totalInputCount = value;
                this.totalInputCountModified = true;
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

                this.totalOutputCount = value;
                this.totalOutputCountModified = true;
            }
        }

        public bool ContainsHeader(UInt256 blockHash)
        {
            CheckTransaction();
            return this.headers.ContainsKey(blockHash);
        }

        public bool TryGetHeader(UInt256 blockHash, out ChainedHeader header)
        {
            CheckTransaction();
            return this.headers.TryGetValue(blockHash, out header);
        }

        public bool TryAddHeader(ChainedHeader header)
        {
            CheckWriteTransaction();

            try
            {
                this.headers.Add(header.Hash, header);
                this.headersModified = true;
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

            var wasRemoved = this.headers.Remove(blockHash);
            if (wasRemoved)
                this.headersModified = true;

            return wasRemoved;
        }

        public bool ContainsUnspentTx(UInt256 txHash)
        {
            CheckTransaction();
            return this.unspentTransactions.ContainsKey(txHash);
        }

        public bool TryGetUnspentTx(UInt256 txHash, out UnspentTx unspentTx)
        {
            CheckTransaction();
            return this.unspentTransactions.TryGetValue(txHash, out unspentTx);
        }

        public bool TryAddUnspentTx(UnspentTx unspentTx)
        {
            CheckWriteTransaction();

            try
            {
                this.unspentTransactions.Add(unspentTx.TxHash, unspentTx);
                this.unspentTxesModified = true;
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

            var wasRemoved = this.unspentTransactions.Remove(txHash);
            if (wasRemoved)
                this.unspentTxesModified = true;

            return wasRemoved;
        }

        public bool TryUpdateUnspentTx(UnspentTx unspentTx)
        {
            CheckWriteTransaction();

            if (this.unspentTransactions.ContainsKey(unspentTx.TxHash))
            {
                this.unspentTransactions[unspentTx.TxHash] = unspentTx;
                this.unspentTxesModified = true;
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
            return this.unspentTransactions.Values;
        }

        public bool ContainsBlockSpentTxes(int blockIndex)
        {
            CheckTransaction();
            return this.blockSpentTxes.ContainsKey(blockIndex);
        }

        public bool TryGetBlockSpentTxes(int blockIndex, out IImmutableList<UInt256> spentTxes)
        {
            CheckTransaction();
            return this.blockSpentTxes.TryGetValue(blockIndex, out spentTxes);
        }

        public bool TryAddBlockSpentTxes(int blockIndex, IImmutableList<UInt256> spentTxes)
        {
            CheckWriteTransaction();

            try
            {
                this.blockSpentTxes.Add(blockIndex, spentTxes);
                this.blockSpentTxesModified = true;
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

            var wasRemoved = this.blockSpentTxes.Remove(blockIndex);
            if (wasRemoved)
                this.blockSpentTxesModified = true;

            return wasRemoved;
        }

        public bool ContainsBlockUnmintedTxes(UInt256 blockHash)
        {
            CheckTransaction();
            return this.blockUnmintedTxes.ContainsKey(blockHash);
        }

        public bool TryGetBlockUnmintedTxes(UInt256 blockHash, out IImmutableList<UnmintedTx> unmintedTxes)
        {
            CheckTransaction();
            return this.blockUnmintedTxes.TryGetValue(blockHash, out unmintedTxes);
        }

        public bool TryAddBlockUnmintedTxes(UInt256 blockHash, IImmutableList<UnmintedTx> unmintedTxes)
        {
            CheckWriteTransaction();

            try
            {
                this.blockUnmintedTxes.Add(blockHash, unmintedTxes);
                this.blockUnmintedTxesModified = true;
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

            var wasRemoved = this.blockUnmintedTxes.Remove(blockHash);
            if (wasRemoved)
                this.blockUnmintedTxesModified = true;

            return wasRemoved;
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
    }
}
