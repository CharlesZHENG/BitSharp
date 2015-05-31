using BitSharp.Common;
using BitSharp.Core.Builders;
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

        private ChainBuilder chain;
        private int? unspentTxCount;
        private int? unspentOutputCount;
        private int? totalTxCount;
        private int? totalInputCount;
        private int? totalOutputCount;
        private ImmutableSortedDictionary<UInt256, UnspentTx>.Builder unspentTransactions;
        private ImmutableDictionary<int, IImmutableList<UInt256>>.Builder blockSpentTxes;
        private ImmutableDictionary<UInt256, IImmutableList<UnmintedTx>>.Builder blockUnmintedTxes;

        private long chainVersion;
        private long unspentTxCountVersion;
        private long unspentOutputCountVersion;
        private long totalTxCountVersion;
        private long totalInputCountVersion;
        private long totalOutputCountVersion;
        private long unspentTxesVersion;
        private long blockSpentTxesVersion;
        private long blockUnmintedTxesVersion;

        private bool chainModified;
        private bool unspentTxCountModified;
        private bool unspentOutputCountModified;
        private bool totalTxCountModified;
        private bool totalInputCountModified;
        private bool totalOutputCountModified;
        private bool unspentTxesModified;
        private bool blockSpentTxesModified;
        private bool blockUnmintedTxesModified;

        internal MemoryChainStateCursor(MemoryChainStateStorage chainStateStorage)
        {
            this.chainStateStorage = chainStateStorage;
        }

        internal ImmutableSortedDictionary<UInt256, UnspentTx>.Builder UnspentTransactionsDictionary { get { return this.unspentTransactions; } }

        public void Dispose()
        {
        }

        public bool InTransaction
        {
            get { return this.inTransaction; }
        }

        public void BeginTransaction(bool readOnly)
        {
            if (this.inTransaction)
                throw new InvalidOperationException();

            this.chainStateStorage.BeginTransaction(out this.chain, out this.unspentTxCount, out this.unspentOutputCount, out this.totalTxCount, out this.totalInputCount, out this.totalOutputCount, out this.unspentTransactions, out this.blockSpentTxes, out this.blockUnmintedTxes, out this.chainVersion, out this.unspentTxCountVersion, out this.unspentOutputCountVersion, out this.totalTxCountVersion, out this.totalInputCountVersion, out this.totalOutputCountVersion, out this.unspentTxesVersion, out this.blockSpentTxesVersion, out this.blockUnmintedTxesVersion);

            this.chainModified = false;
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
                this.chainModified ? this.chain : null,
                this.unspentTxCountModified ? this.unspentTxCount : null,
                this.unspentOutputCountModified ? this.unspentOutputCount : null,
                this.totalTxCountModified ? this.totalTxCount : null,
                this.totalInputCountModified ? this.totalInputCount : null,
                this.totalOutputCountModified ? this.totalOutputCount : null,
                this.unspentTxesModified ? this.unspentTransactions : null,
                this.blockSpentTxesModified ? this.blockSpentTxes : null,
                this.blockUnmintedTxesModified ? this.blockUnmintedTxes : null,
                this.chainVersion, this.unspentTxCountVersion, this.unspentOutputCountVersion, this.totalTxCountVersion, this.totalInputCountVersion, this.totalOutputCountVersion, this.unspentTxesVersion, this.blockSpentTxesVersion, this.blockUnmintedTxesVersion);

            this.chain = null;
            this.unspentTxCount = null;
            this.unspentOutputCount = null;
            this.totalTxCount = null;
            this.totalInputCount = null;
            this.totalOutputCount = null;
            this.unspentTransactions = null;
            this.blockSpentTxes = null;
            this.blockUnmintedTxes = null;

            this.inTransaction = false;
        }

        public void RollbackTransaction()
        {
            if (!this.inTransaction)
                throw new InvalidOperationException();

            this.chain = null;
            this.unspentTxCount = null;
            this.unspentOutputCount = null;
            this.totalTxCount = null;
            this.totalInputCount = null;
            this.totalOutputCount = null;
            this.unspentTransactions = null;
            this.blockSpentTxes = null;
            this.blockUnmintedTxes = null;

            this.inTransaction = false;
        }

        public IEnumerable<ChainedHeader> ReadChain()
        {
            if (this.inTransaction)
                return this.chain.Blocks;
            else
                return this.chainStateStorage.ReadChain();
        }

        public ChainedHeader GetChainTip()
        {
            if (this.inTransaction)
                return this.chain.LastBlock;
            else
                return this.chainStateStorage.GetChainTip();
        }

        public void AddChainedHeader(ChainedHeader chainedHeader)
        {
            if (this.inTransaction)
            {
                this.chain.AddBlock(chainedHeader);
                this.chainModified = true;
            }
            else
            {
                this.chainStateStorage.AddChainedHeader(chainedHeader);
            }
        }

        public void RemoveChainedHeader(ChainedHeader chainedHeader)
        {
            if (this.inTransaction)
            {
                this.chain.RemoveBlock(chainedHeader);
                this.chainModified = true;
            }
            else
            {
                this.chainStateStorage.RemoveChainedHeader(chainedHeader);
            }
        }

        public int UnspentTxCount
        {
            get
            {
                if (this.inTransaction)
                    return this.unspentTxCount.Value;
                else
                    return this.chainStateStorage.UnspentTxCount;
            }
            set
            {
                if (this.inTransaction)
                {
                    this.unspentTxCount = value;
                    this.unspentTxCountModified = true;
                }
                else
                    this.chainStateStorage.UnspentTxCount = value;
            }
        }

        public int UnspentOutputCount
        {
            get
            {
                if (this.inTransaction)
                    return this.unspentOutputCount.Value;
                else
                    return this.chainStateStorage.UnspentOutputCount;
            }
            set
            {
                if (this.inTransaction)
                {
                    this.unspentOutputCount = value;
                    this.unspentOutputCountModified = true;
                }
                else
                    this.chainStateStorage.UnspentOutputCount = value;
            }
        }

        public int TotalTxCount
        {
            get
            {
                if (this.inTransaction)
                    return this.totalTxCount.Value;
                else
                    return this.chainStateStorage.TotalTxCount;
            }
            set
            {
                if (this.inTransaction)
                {
                    this.totalTxCount = value;
                    this.totalTxCountModified = true;
                }
                else
                    this.chainStateStorage.TotalTxCount = value;
            }
        }

        public int TotalInputCount
        {
            get
            {
                if (this.inTransaction)
                    return this.totalInputCount.Value;
                else
                    return this.chainStateStorage.TotalInputCount;
            }
            set
            {
                if (this.inTransaction)
                {
                    this.totalInputCount = value;
                    this.totalInputCountModified = true;
                }
                else
                    this.chainStateStorage.TotalInputCount = value;
            }
        }

        public int TotalOutputCount
        {
            get
            {
                if (this.inTransaction)
                    return this.totalOutputCount.Value;
                else
                    return this.chainStateStorage.TotalOutputCount;
            }
            set
            {
                if (this.inTransaction)
                {
                    this.totalOutputCount = value;
                    this.totalOutputCountModified = true;
                }
                else
                    this.chainStateStorage.TotalOutputCount = value;
            }
        }

        public bool ContainsUnspentTx(UInt256 txHash)
        {
            if (this.inTransaction)
                return this.unspentTransactions.ContainsKey(txHash);
            else
                return this.chainStateStorage.ContainsUnspentTx(txHash);
        }

        public bool TryGetUnspentTx(UInt256 txHash, out UnspentTx unspentTx)
        {
            if (this.inTransaction)
                return this.unspentTransactions.TryGetValue(txHash, out unspentTx);
            else
                return this.chainStateStorage.TryGetUnspentTx(txHash, out unspentTx);
        }

        public bool TryAddUnspentTx(UnspentTx unspentTx)
        {
            if (this.inTransaction)
            {
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
            else
            {
                return this.chainStateStorage.TryAddUnspentTx(unspentTx);
            }
        }

        public bool TryRemoveUnspentTx(UInt256 txHash)
        {
            if (this.inTransaction)
            {
                var wasRemoved = this.unspentTransactions.Remove(txHash);
                if (wasRemoved)
                    this.unspentTxesModified = true;

                return wasRemoved;
            }
            else
            {
                return this.chainStateStorage.TryRemoveUnspentTx(txHash);
            }
        }

        public bool TryUpdateUnspentTx(UnspentTx unspentTx)
        {
            if (this.inTransaction)
            {
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
            else
            {
                return this.chainStateStorage.TryUpdateUnspentTx(unspentTx);
            }
        }

        public IEnumerable<UnspentTx> ReadUnspentTransactions()
        {
            if (this.inTransaction)
                return this.unspentTransactions.Values;
            else
                return this.chainStateStorage.ReadUnspentTransactions();
        }

        public bool ContainsBlockSpentTxes(int blockIndex)
        {
            if (this.inTransaction)
                return this.blockSpentTxes.ContainsKey(blockIndex);
            else
                return this.chainStateStorage.ContainsBlockSpentTxes(blockIndex);
        }

        public bool TryGetBlockSpentTxes(int blockIndex, out IImmutableList<UInt256> spentTxes)
        {
            if (this.inTransaction)
            {
                return this.blockSpentTxes.TryGetValue(blockIndex, out spentTxes);
            }
            else
            {
                return this.chainStateStorage.TryGetBlockSpentTxes(blockIndex, out spentTxes);
            }
        }

        public bool TryAddBlockSpentTxes(int blockIndex, IImmutableList<UInt256> spentTxes)
        {
            if (this.inTransaction)
            {
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
            else
            {
                return this.chainStateStorage.TryAddBlockSpentTxes(blockIndex, spentTxes);
            }
        }

        public bool TryRemoveBlockSpentTxes(int blockIndex)
        {
            if (this.inTransaction)
            {
                var wasRemoved = this.blockSpentTxes.Remove(blockIndex);
                if (wasRemoved)
                    this.blockSpentTxesModified = true;

                return wasRemoved;
            }
            else
            {
                return this.chainStateStorage.TryRemoveBlockSpentTxes(blockIndex);
            }
        }

        public bool ContainsBlockUnmintedTxes(UInt256 blockHash)
        {
            if (this.inTransaction)
                return this.blockUnmintedTxes.ContainsKey(blockHash);
            else
                return this.chainStateStorage.ContainsBlockUnmintedTxes(blockHash);
        }

        public bool TryGetBlockUnmintedTxes(UInt256 blockHash, out IImmutableList<UnmintedTx> unmintedTxes)
        {
            if (this.inTransaction)
            {
                return this.blockUnmintedTxes.TryGetValue(blockHash, out unmintedTxes);
            }
            else
            {
                return this.chainStateStorage.TryGetBlockUnmintedTxes(blockHash, out unmintedTxes);
            }
        }

        public bool TryAddBlockUnmintedTxes(UInt256 blockHash, IImmutableList<UnmintedTx> unmintedTxes)
        {
            if (this.inTransaction)
            {
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
            else
            {
                return this.chainStateStorage.TryAddBlockUnmintedTxes(blockHash, unmintedTxes);
            }
        }

        public bool TryRemoveBlockUnmintedTxes(UInt256 blockHash)
        {
            if (this.inTransaction)
            {
                var wasRemoved = this.blockUnmintedTxes.Remove(blockHash);
                if (wasRemoved)
                    this.blockUnmintedTxesModified = true;

                return wasRemoved;
            }
            else
            {
                return this.chainStateStorage.TryRemoveBlockUnmintedTxes(blockHash);
            }
        }

        public void Flush()
        {
        }

        public void Defragment()
        {
        }
    }
}
