using BitSharp.Common;

namespace BitSharp.Core.Domain
{
    /// <summary>
    /// Represents a transaction that has been fully spent and removed from the UTXO.
    /// 
    /// The spent transaction information is needed when there is a chain state rollback and when pruning spent transactions from blocks.
    /// </summary>
    public class SpentTx
    {
        private readonly UInt256 txHash;
        private readonly int confirmedBlockIndex;
        private readonly int txIndex;
        private readonly int outputCount;
        private readonly int spentBlockIndex;

        public SpentTx(UInt256 txHash, int confirmedBlockIndex, int txIndex, int outputCount, int spentBlockIndex)
        {
            this.txHash = txHash;
            this.confirmedBlockIndex = confirmedBlockIndex;
            this.txIndex = txIndex;
            this.outputCount = outputCount;
            this.spentBlockIndex = spentBlockIndex;
        }

        /// <summary>
        /// The transaction's hash.
        /// </summary>
        public UInt256 TxHash { get { return this.txHash; } }

        /// <summary>
        /// The block index (height) where the transaction was initially confirmed.
        /// </summary>
        public int ConfirmedBlockIndex { get { return this.confirmedBlockIndex; } }

        /// <summary>
        /// The transaction's index within its confirming block.
        /// </summary>
        public int TxIndex { get { return this.txIndex; } }

        /// <summary>
        /// The total number of outputs the transaction contained, spent or unspent.
        /// </summary>
        public int OutputCount { get { return this.outputCount; } }

        /// <summary>
        /// The block index (height) where the transaction was fully spent.
        /// </summary>
        public int SpentBlockIndex { get { return this.spentBlockIndex; } }

        public override bool Equals(object obj)
        {
            if (!(obj is SpentTx))
                return false;

            var other = (SpentTx)obj;
            return other.txHash == this.txHash && other.confirmedBlockIndex == this.confirmedBlockIndex && other.txIndex == this.txIndex && other.outputCount == this.outputCount && other.spentBlockIndex == this.spentBlockIndex;
        }

        public override int GetHashCode()
        {
            return this.txHash.GetHashCode() ^ this.confirmedBlockIndex.GetHashCode() ^ this.txIndex.GetHashCode() ^ this.outputCount.GetHashCode();
        }
    }
}
