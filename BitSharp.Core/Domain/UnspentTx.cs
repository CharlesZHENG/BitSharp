using BitSharp.Common;
using System;
using System.Linq;

namespace BitSharp.Core.Domain
{
    /// <summary>
    /// Represents a transaction that has unspent outputs in the UTXO.
    /// </summary>
    public class UnspentTx
    {
        private readonly UInt256 txHash;
        private readonly int blockIndex;
        private readonly int txIndex;
        private readonly OutputStates outputStates;

        public UnspentTx(UInt256 txHash, int blockIndex, int txIndex, OutputStates outputStates)
        {
            this.txHash = txHash;
            this.blockIndex = blockIndex;
            this.txIndex = txIndex;
            this.outputStates = outputStates;
        }

        public UnspentTx(UInt256 txHash, int blockIndex, int txIndex, int length, OutputState state)
        {
            this.txHash = txHash;
            this.blockIndex = blockIndex;
            this.txIndex = txIndex;
            this.outputStates = new OutputStates(length, state);
        }

        /// <summary>
        /// The transaction's hash.
        /// </summary>
        public UInt256 TxHash { get { return this.txHash; } }

        /// <summary>
        /// The block index (height) where the transaction was initially confirmed.
        /// </summary>
        public int BlockIndex { get { return this.blockIndex; } }

        /// <summary>
        /// The transaction's index within its confirming block.
        /// </summary>
        public int TxIndex { get { return this.txIndex; } }

        /// <summary>
        /// The spent/unspent state of each of the transaction's outputs.
        /// </summary>
        public OutputStates OutputStates { get { return this.outputStates; } }

        /// <summary>
        /// True if all of the transaction's outputs are in the spent state.
        /// </summary>
        public bool IsFullySpent { get { return this.OutputStates.All(x => x == OutputState.Spent); } }

        public UnspentTx SetOutputState(int index, OutputState value)
        {
            return new UnspentTx(this.txHash, this.blockIndex, this.txIndex, this.outputStates.Set(index, value));
        }

        /// <summary>
        /// Create a spent transaction representation of this unspent transaction.
        /// </summary>
        /// <returns>The spent transaction.</returns>
        public SpentTx ToSpentTx()
        {
            if (!this.IsFullySpent)
                throw new InvalidOperationException();
            
            return new SpentTx(this.txHash, this.blockIndex, this.txIndex);
        }

        public override bool Equals(object obj)
        {
            if (!(obj is UnspentTx))
                return false;

            var other = (UnspentTx)obj;
            return other.blockIndex == this.blockIndex && other.txIndex == this.txIndex && other.outputStates.Equals(this.outputStates);
        }

        public override int GetHashCode()
        {
            return this.blockIndex.GetHashCode() ^ this.txIndex.GetHashCode() ^ this.outputStates.GetHashCode();
        }
    }
}
