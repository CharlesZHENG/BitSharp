using BitSharp.Common;
using BitSharp.Core.Domain;
using System;
using System.Collections.Generic;

namespace BitSharp.Core.Storage
{
    public interface IBlockTxesStorage : IDisposable
    {
        /// <summary>
        /// Retrieves the number of blocks with transaction data stored. Pruned blocks are included.
        /// </summary>
        int BlockCount { get; }

        /// <summary>
        /// Determines whether transaction data exists for a block. Pruned blocks are included.
        /// </summary>
        /// <param name="blockHash">The block's hash.</param>
        /// <returns>true if transacton data is present; otherwise, false</returns>
        bool ContainsBlock(UInt256 blockHash);

        /// <summary>
        /// Add a block's transactions to storage.
        /// </summary>
        /// <param name="blockHash">The block's hash.</param>
        /// <param name="transactions">An enumerable of the block's transactions.</param>
        /// <returns>true if the transaction data was added; otherwise, false</returns>
        bool TryAddBlockTransactions(UInt256 blockHash, IEnumerable<Transaction> transactions);

        /// <summary>
        /// Retrieve a transaction from a block.
        /// </summary>
        /// <param name="blockHash">The block hash of the block containing the transaction.</param>
        /// <param name="txIndex">The transaction index of the transaction inside the block.</param>
        /// <param name="transaction">Contains the retrieved transaction when successful; otherwise, null.</param>
        /// <returns>true if the transaction was retrieved; otherwise, false</returns>
        bool TryGetTransaction(UInt256 blockHash, int txIndex, out Transaction transaction);

        /// <summary>
        /// Remove all transaction data for a block.
        /// </summary>
        /// <param name="blockHash">The block's hash.</param>
        /// <returns>true if transaction data was removed; otherwise, false</returns>
        bool TryRemoveBlockTransactions(UInt256 blockHash);

        /// <summary>
        /// Read the transaction data for a block, including pruning information.
        /// </summary>
        /// <param name="blockHash">The block's hash.</param>
        /// <param name="transaction">Contains the retrieved block's transactions when successful; otherwise, null.</param>
        /// <returns>true if the block's transactions were retrieved; otherwise, false</returns>
        bool TryReadBlockTransactions(UInt256 blockHash, out IEnumerable<BlockTx> blockTxes);

        /// <summary>
        /// Remove raw transaction data and prune the merkle tree for all transactions indicated by <paramref name="txIndices"/> within a block.
        /// </summary>
        /// <param name="blockHash">The block's hash.</param>
        /// <param name="txIndices">An enumerable of the transaction indices for the transactions to be removed.</param>
        void PruneElements(UInt256 blockHash, IEnumerable<int> txIndices);

        /// <summary>
        /// Fully flush storage.
        /// </summary>
        void Flush();

        //TODO keep this here? IStorageManager?
        void Defragment();
    }
}
