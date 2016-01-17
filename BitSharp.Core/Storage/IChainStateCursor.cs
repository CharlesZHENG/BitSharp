using BitSharp.Common;
using BitSharp.Core.Domain;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading.Tasks;

namespace BitSharp.Core.Storage
{
    /// <summary>
    /// Represents an open cursor/connection into chain state storage.
    /// 
    /// A chain state cursor is for use by a single thread.
    /// </summary>
    public interface IChainStateCursor : IDisposable
    {
        /// <summary>
        /// Whether the cursor is currently in a transaction.
        /// </summary>
        bool InTransaction { get; }

        /// <summary>
        /// Begin a new transaction.
        /// </summary>
        void BeginTransaction(bool readOnly = false);

        /// <summary>
        /// Commit the current transaction.
        /// </summary>
        void CommitTransaction();

        Task CommitTransactionAsync();

        /// <summary>
        /// Rollback the current transaction
        /// </summary>
        void RollbackTransaction();

        /// <summary>
        /// Retrieve the tip of the chain.
        /// </summary>
        /// <returns>The chained header for the tip, or null for an empty chain.</returns>
        ChainedHeader ChainTip { get; set; }

        /// <summary>
        /// The current unspent transaction count.
        /// </summary>
        int UnspentTxCount { get; set; }

        int UnspentOutputCount { get; set; }

        int TotalTxCount { get; set; }

        int TotalInputCount { get; set; }

        int TotalOutputCount { get; set; }

        bool ContainsHeader(UInt256 blockHash);

        bool TryGetHeader(UInt256 blockHash, out ChainedHeader header);

        bool TryAddHeader(ChainedHeader header);

        bool TryRemoveHeader(UInt256 txHash);

        /// <summary>
        /// Determine whether an unspent transaction is present.
        /// </summary>
        /// <param name="txHash">The transaction's hash.</param>
        /// <returns>true if the transaction is present; otherwise, false</returns>
        bool ContainsUnspentTx(UInt256 txHash);

        /// <summary>
        /// Retreive an unspent transaction.
        /// </summary>
        /// <param name="txHash">The transaction's hash.</param>
        /// <param name="unspentTx">Contains the retrieved transaction when successful; otherwise, null.</param>
        /// <returns>true if the transaction was retrieved; otherwise, false</returns>
        bool TryGetUnspentTx(UInt256 txHash, out UnspentTx unspentTx);

        /// <summary>
        /// Add an unspent transaction.
        /// </summary>
        /// <param name="unspentTx">The unspent transaction.</param>
        /// <returns>true if the transaction was added; otherwise, false</returns>
        bool TryAddUnspentTx(UnspentTx unspentTx);

        /// <summary>
        /// Remove an unspent transaction.
        /// </summary>
        /// <param name="txHash">The transaction's hash.</param>
        /// <returns>true if the transaction was removed; otherwise, false</returns>
        bool TryRemoveUnspentTx(UInt256 txHash);

        void RemoveUnspentTx(UInt256 txHash);

        /// <summary>
        /// Update an unspent transaction.
        /// </summary>
        /// <param name="unspentTx">The unspent transaction.</param>
        /// <returns>true if the transaction was updated; otherwise, false</returns>
        bool TryUpdateUnspentTx(UnspentTx unspentTx);

        //TODO
        IEnumerable<UnspentTx> ReadUnspentTransactions();

        bool ContainsUnspentTxOutput(TxOutputKey txOutputKey);

        bool TryGetUnspentTxOutput(TxOutputKey txOutputKey, out TxOutput txOutput);

        bool TryAddUnspentTxOutput(TxOutputKey txOutputKey, TxOutput txOutput);

        bool TryRemoveUnspentTxOutput(TxOutputKey txOutputKey);

        void RemoveUnspentTxOutput(TxOutputKey txOutputKey);

        /// <summary>
        /// Determine whether spent transactions are present for a block.
        /// </summary>
        /// <param name="blockIndex">The block's index (height) in the chain.</param>
        /// <returns>true if the block's spent transactions are present; otherwise, false</returns>
        bool ContainsBlockSpentTxes(int blockIndex);

        /// <summary>
        /// Retreive a block's spent transactions.
        /// </summary>
        /// <param name="blockIndex">The block's index (height) in the chain.</param>
        /// <param name="spentTxes">Contains the spent transactions when successful; otherwise, null.</param>
        /// <returns>true if the block's spent transactions were retrieved; otherwise, false</returns>
        bool TryGetBlockSpentTxes(int blockIndex, out BlockSpentTxes spentTxes);

        /// <summary>
        /// Add a block's spent transactions.
        /// </summary>
        /// <param name="blockIndex">The block's index (height) in the chain.</param>
        /// <param name="spentTxes">The spent transactions.</param>
        /// <returns>true if the block's spent transactions were added; otherwise, false</returns>
        bool TryAddBlockSpentTxes(int blockIndex, BlockSpentTxes spentTxes);

        /// <summary>
        /// Remove a block's spent transactions.
        /// </summary>
        /// <param name="blockIndex">The block's index (height) in the chain.</param>
        /// <returns>true if the block's spent transactions were removed; otherwise, false</returns>
        bool TryRemoveBlockSpentTxes(int blockIndex);

        /// <summary>
        /// Determine whether unminted transactions are present for a block.
        /// </summary>
        /// <param name="blockHash">The block's hash.</param>
        /// <returns>true if the block's unminted transactions are present; otherwise, false</returns>
        bool ContainsBlockUnmintedTxes(UInt256 blockHash);

        /// <summary>
        /// Retreive a block's unminted transactions.
        /// </summary>
        /// <param name="blockHash">The block's hash.</param>
        /// <param name="unmintedTxes">Contains the unminted transactions when successful; otherwise, null.</param>
        /// <returns>true if the block's unminted transactions were retrieved; otherwise, false</returns>
        bool TryGetBlockUnmintedTxes(UInt256 blockHash, out IImmutableList<UnmintedTx> unmintedTxes);

        /// <summary>
        /// Add a block's unminted transactions.
        /// </summary>
        /// <param name="blockHash">The block's hash.</param>
        /// <param name="unmintedTxes">The unminted transactions.</param>
        /// <returns>true if the block's unminted transactions were added; otherwise, false</returns>
        bool TryAddBlockUnmintedTxes(UInt256 blockHash, IImmutableList<UnmintedTx> unmintedTxes);

        /// <summary>
        /// Remove a block's unminted transactions.
        /// </summary>
        /// <param name="blockHash">The block's hash.</param>
        /// <returns>true if the block's unminted transactions were removed; otherwise, false</returns>
        bool TryRemoveBlockUnmintedTxes(UInt256 blockHash);

        /// <summary>
        /// Fully flush storage.
        /// </summary>
        void Flush();

        //TODO
        void Defragment();
    }
}
