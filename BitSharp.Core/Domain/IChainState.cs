using BitSharp.Common;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace BitSharp.Core.Domain
{
    /// <summary>
    /// Represents an immutable snapshot of the chain state at a given block.
    /// 
    /// All methods are thread safe.
    /// </summary>
    public interface IChainState : IDisposable
    {
        int CursorCount { get; }

        /// <summary>
        /// The full chain for this chain state.
        /// </summary>
        Chain Chain { get; }

        /// <summary>
        /// The current unspent transaction count.
        /// </summary>
        int UnspentTxCount { get; }

        int UnspentOutputCount { get; }

        int TotalTxCount { get; }

        int TotalInputCount { get; }

        int TotalOutputCount { get; }

        bool ContainsHeader(UInt256 blockHash);

        bool TryGetHeader(UInt256 blockHash, out ChainedHeader header);

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

        //TODO
        IEnumerable<UnspentTx> ReadUnspentTransactions();

        bool ContainsUnspentTxOutput(TxOutputKey txOutputKey);

        bool TryGetUnspentTxOutput(TxOutputKey txOutputKey, out TxOutput txOutput);

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
    }

    public static class IChainStateExtensions
    {
        public static bool CanSpend(this IChainState chainState, TxOutputKey txOutputKey)
        {
            if (txOutputKey == null)
                throw new ArgumentNullException("prevTxOutput");

            UnspentTx unspentTx;
            if (chainState.TryGetUnspentTx(txOutputKey.TxHash, out unspentTx))
            {
                var outputIndex = unchecked((int)txOutputKey.TxOutputIndex);

                if (outputIndex < 0 || outputIndex >= unspentTx.OutputStates.Length)
                    return false;

                return unspentTx.OutputStates[outputIndex] == OutputState.Unspent;
            }
            else
            {
                return false;
            }
        }
    }
}
