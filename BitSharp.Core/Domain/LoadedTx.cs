using BitSharp.Common.ExtensionMethods;
using System.Collections.Immutable;

namespace BitSharp.Core.Domain
{
    /// <summary>
    /// Represents a fully loaded transaction, with each input's previous transaction included.
    /// </summary>
    public class LoadedTx
    {
        private readonly Transaction transaction;
        private readonly int txIndex;
        private readonly ImmutableArray<Transaction> inputTxes;

        /// <summary>
        /// Initializes a new instance of <see cref="LoadedTx"/> with the specified transaction and each input's previous transaction.
        /// </summary>
        /// <param name="transaction">The transaction.</param>
        /// <param name="txIndex">The index of the transaction.</param>
        /// <param name="inputTxes">The array of transactions corresponding to each input's previous transaction.</param>
        public LoadedTx(Transaction transaction, int txIndex, ImmutableArray<Transaction> inputTxes)
        {
            this.transaction = transaction;
            this.txIndex = txIndex;
            this.inputTxes = inputTxes;
        }

        /// <summary>
        /// Gets the transaction.
        /// </summary>
        public Transaction Transaction { get { return this.transaction; } }

        /// <summary>
        /// Gets the index of the transaction.
        /// </summary>
        public int TxIndex { get { return this.txIndex; } }

        /// <summary>
        /// Gets array of transactions corresponding to each input's previous transaction.
        /// </summary>
        public ImmutableArray<Transaction> InputTxes { get { return this.inputTxes; } }

        /// <summary>
        /// Get the previous transaction output for the specified input.
        /// </summary>
        /// <param name="inputIndex">The index of the input.</param>
        /// <returns>The input's previous transaction output.</returns>
        public TxOutput GetInputPrevTxOutput(int inputIndex)
        {
            return this.inputTxes[inputIndex].Outputs[this.transaction.Inputs[inputIndex].PreviousTxOutputKey.TxOutputIndex.ToIntChecked()];
        }
    }
}
