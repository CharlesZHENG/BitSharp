using BitSharp.Common;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Core.Domain
{
    /// <summary>
    /// Represents a transaction that has unspent outputs in the UTXO.
    /// </summary>
    public class UnspentTx
    {
        public UnspentTx(UInt256 txHash, int blockIndex, int txIndex, uint txVersion, bool isCoinbase, OutputStates outputStates)
        {
            TxHash = txHash;
            BlockIndex = blockIndex;
            TxIndex = txIndex;
            TxVersion = txVersion;
            IsCoinbase = isCoinbase;
            OutputStates = outputStates;
            IsFullySpent = OutputStates.All(x => x == OutputState.Spent);
        }

        public UnspentTx(UInt256 txHash, int blockIndex, int txIndex, uint txVersion, bool isCoinbase, int length, OutputState state)
            : this(txHash, blockIndex, txIndex, txVersion, isCoinbase, new OutputStates(length, state))
        { }

        /// <summary>
        /// The transaction's hash.
        /// </summary>
        public UInt256 TxHash { get; }

        /// <summary>
        /// The block index (height) where the transaction was initially confirmed.
        /// </summary>
        public int BlockIndex { get; }

        /// <summary>
        /// The transaction's index within its confirming block.
        /// </summary>
        public int TxIndex { get; }

        public uint TxVersion { get; }

        public bool IsCoinbase { get; }

        /// <summary>
        /// The spent/unspent state of each of the transaction's outputs.
        /// </summary>
        public OutputStates OutputStates { get; }

        /// <summary>
        /// True if all of the transaction's outputs are in the spent state.
        /// </summary>
        public bool IsFullySpent { get; }

        public UnspentTx SetOutputState(int index, OutputState value)
        {
            return new UnspentTx(this.TxHash, this.BlockIndex, this.TxIndex, this.TxVersion, this.IsCoinbase, this.OutputStates.Set(index, value));
        }

        /// <summary>
        /// Create a spent transaction representation of this unspent transaction.
        /// </summary>
        /// <returns>The spent transaction.</returns>
        public SpentTx ToSpentTx()
        {
            if (!this.IsFullySpent)
                throw new InvalidOperationException();

            return new SpentTx(this.TxHash, this.BlockIndex, this.TxIndex, this.OutputStates.Length);
        }

        //TODO only exists for tests
        public override bool Equals(object obj)
        {
            if (!(obj is UnspentTx))
                return false;

            var other = (UnspentTx)obj;
            return other.TxHash == this.TxHash && other.BlockIndex == this.BlockIndex && other.TxIndex == this.TxIndex && other.TxVersion == this.TxVersion && other.IsCoinbase == this.IsCoinbase && other.OutputStates.Equals(this.OutputStates);
        }

        public override int GetHashCode()
        {
            return this.TxHash.GetHashCode() ^ this.BlockIndex.GetHashCode() ^ this.TxIndex.GetHashCode() ^ this.TxVersion.GetHashCode() ^ this.IsCoinbase.GetHashCode() ^ this.OutputStates.GetHashCode();
        }
    }
}
