﻿using BitSharp.Common;
using BitSharp.Core.Domain;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
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
        void BeginTransaction();

        /// <summary>
        /// Commit the current transaction.
        /// </summary>
        void CommitTransaction();

        /// <summary>
        /// Rollback the current transaction
        /// </summary>
        void RollbackTransaction();

        /// <summary>
        /// Enumerate the chain's headers.
        /// </summary>
        /// <returns>An enumerable of the chain's headers</returns>
        IEnumerable<ChainedHeader> ReadChain();

        /// <summary>
        /// Retrieve the tip of the chain.
        /// </summary>
        /// <returns>The chained header for the tip, or null for an empty chain.</returns>
        ChainedHeader GetChainTip();

        /// <summary>
        /// Add a new header to the chain.
        /// </summary>
        /// <param name="chainedHeader">The header to add.</param>
        void AddChainedHeader(ChainedHeader chainedHeader);

        /// <summary>
        /// Remove a header from the chain.
        /// </summary>
        /// <param name="chainedHeader">The header to remove.</param>
        void RemoveChainedHeader(ChainedHeader chainedHeader);

        /// <summary>
        /// The current unspent transaction count.
        /// </summary>
        int UnspentTxCount { get; }

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

        /// <summary>
        /// Update an unspent transaction.
        /// </summary>
        /// <param name="unspentTx">The unspent transaction.</param>
        /// <returns>true if the transaction was updated; otherwise, false</returns>
        bool TryUpdateUnspentTx(UnspentTx unspentTx);

        //TODO
        IEnumerable<UnspentTx> ReadUnspentTransactions();

        /// <summary>
        /// Determine whether spent transactions are present for a block.
        /// </summary>
        /// <param name="blockIndex">The block's index (height) in the chain.</param>
        /// <returns>true if the block's spent transactions are present; otherwise, false</returns>
        bool ContainsBlockSpentTxes(int blockIndex);

        /// <summary>
        /// Retreive a block's spent transactions.
        /// </summary>
        /// <param name="blockIndex">The blocks's index (height) in the chain.</param>
        /// <param name="spentTxesBytes">Contains the spent transactions when successful; otherwise, null.</param>
        /// <returns>true if the block's spent transactions were retrieved; otherwise, false</returns>
        bool TryGetBlockSpentTxes(int blockIndex, out IImmutableList<SpentTx> spentTxes);

        /// <summary>
        /// Add a block's spent transactions.
        /// </summary>
        /// <param name="blockIndex">The blocks's index (height) in the chain.</param>
        /// <param name="spentTxesBytes">The spent transactions.</param>
        /// <returns>true if the block's spent transactions were added; otherwise, false</returns>
        bool TryAddBlockSpentTxes(int blockIndex, IImmutableList<SpentTx> spentTxes);

        /// <summary>
        /// Remove a block's spent transactions.
        /// </summary>
        /// <param name="blockIndex">The blocks's index (height) in the chain.</param>
        /// <returns>true if the block's spent transactions were removed; otherwise, false</returns>
        bool TryRemoveBlockSpentTxes(int blockIndex);

        //TODO
        void RemoveSpentTransactionsToHeight(int spentBlockIndex);

        /// <summary>
        /// Fully flush storage.
        /// </summary>
        void Flush();

        //TODO
        void Defragment();
    }
}
