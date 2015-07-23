using BitSharp.Common.ExtensionMethods;
using System.Collections.Immutable;

namespace BitSharp.Core.Domain
{
    /// <summary>
    /// Represents a fully loaded transaction, with each input's previous transaction included.
    /// </summary>
    public class LoadedTx
    {
        /// <summary>
        /// Initializes a new instance of <see cref="LoadedTx"/> with the specified transaction and each input's previous transaction.
        /// </summary>
        /// <param name="transaction">The transaction.</param>
        /// <param name="txIndex">The index of the transaction.</param>
        /// <param name="inputTxes">The array of transactions corresponding to each input's previous transaction.</param>
        public LoadedTx(Transaction transaction, int txIndex, ImmutableArray<Transaction> inputTxes)
        {
            Transaction = transaction;
            TxIndex = txIndex;
            InputTxes = inputTxes;
        }

        /// <summary>
        /// Indicates whether this is the coinbase transaction.
        /// </summary>
        public bool IsCoinbase => this.TxIndex == 0;

        /// <summary>
        /// Gets the transaction.
        /// </summary>
        public Transaction Transaction { get; }

        /// <summary>
        /// Gets the index of the transaction.
        /// </summary>
        public int TxIndex { get; }

        /// <summary>
        /// Gets array of transactions corresponding to each input's previous transaction.
        /// </summary>
        public ImmutableArray<Transaction> InputTxes { get; }

        /// <summary>
        /// Get the previous transaction output for the specified input.
        /// </summary>
        /// <param name="inputIndex">The index of the input.</param>
        /// <returns>The input's previous transaction output.</returns>
        public TxOutput GetInputPrevTxOutput(int inputIndex)
        {
            return this.InputTxes[inputIndex].Outputs[this.Transaction.Inputs[inputIndex].PreviousTxOutputKey.TxOutputIndex.ToIntChecked()];
        }
    }
}
