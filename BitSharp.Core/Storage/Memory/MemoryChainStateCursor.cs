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
        private ImmutableSortedDictionary<UInt256, UnspentTx>.Builder unspentTransactions;
        private ImmutableDictionary<int, IImmutableList<SpentTx>>.Builder blockSpentTxes;
        private ImmutableDictionary<UInt256, IImmutableList<UnmintedTx>>.Builder blockUnmintedTxes;

        private long chainVersion;
        private long unspentTxesVersion;
        private long spentTxesVersion;
        private long unmintedTxesVersion;

        private bool chainModified;
        private bool unspentTxesModified;
        private bool spentTxesModified;
        private bool unmintedTxesModified;

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

        public void BeginTransaction()
        {
            if (this.inTransaction)
                throw new InvalidOperationException();

            this.chainStateStorage.BeginTransaction(out this.chain, out this.unspentTxCount, out this.unspentTransactions, out this.blockSpentTxes, out this.blockUnmintedTxes, out this.chainVersion, out this.unspentTxesVersion, out this.spentTxesVersion, out this.unmintedTxesVersion);

            this.chainModified = false;
            this.unspentTxesModified = false;
            this.spentTxesModified = false;
            this.unmintedTxesModified = false;

            this.inTransaction = true;
        }

        public void CommitTransaction()
        {
            if (!this.inTransaction)
                throw new InvalidOperationException();

            this.chainStateStorage.CommitTransaction(
                this.chainModified ? this.chain : null,
                this.unspentTxesModified ? this.unspentTxCount : null,
                this.unspentTxesModified ? this.unspentTransactions : null,
                this.spentTxesModified ? this.blockSpentTxes : null,
                this.unmintedTxesModified ? this.blockUnmintedTxes : null,
                this.chainVersion, this.unspentTxesVersion, this.spentTxesVersion, this.unmintedTxesVersion);

            this.chain = null;
            this.unspentTxCount = null;
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
                    this.unspentTxesModified = true;
                }
                else
                    this.chainStateStorage.UnspentTxCount = value;
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

        public bool TryGetBlockSpentTxes(int blockIndex, out IImmutableList<SpentTx> spentTxes)
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

        public bool TryAddBlockSpentTxes(int blockIndex, IImmutableList<SpentTx> spentTxes)
        {
            if (this.inTransaction)
            {
                try
                {
                    this.blockSpentTxes.Add(blockIndex, spentTxes);
                    this.spentTxesModified = true;
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
                    this.spentTxesModified = true;

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
                    this.unmintedTxesModified = true;
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
                    this.unmintedTxesModified = true;

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
