using BitSharp.Common;

namespace BitSharp.Core.Domain
{
    /// <summary>
    /// Represents a transaction that has been fully spent and removed from the UTXO.
    /// 
    /// The spent transaction information is needed when pruning spent transactions from the tx index and from blocks.
    /// </summary>
    public class SpentTx
    {
        public SpentTx(UInt256 txHash, int confirmedBlockIndex, int txIndex, int outputCount)
        {
            TxHash = txHash;
            ConfirmedBlockIndex = confirmedBlockIndex;
            TxIndex = txIndex;
            OutputCount = outputCount;
        }

        /// <summary>
        /// The transaction's hash.
        /// </summary>
        public UInt256 TxHash { get; }

        /// <summary>
        /// The block index (height) where the transaction was initially confirmed.
        /// </summary>
        public int ConfirmedBlockIndex { get; }

        /// <summary>
        /// The transaction's index within its confirming block.
        /// </summary>
        public int TxIndex { get; }

        public int OutputCount { get; }

        //TODO only exists for tests
        public override bool Equals(object obj)
        {
            if (!(obj is SpentTx))
                return false;

            var other = (SpentTx)obj;
            return other.TxHash == this.TxHash && other.ConfirmedBlockIndex == this.ConfirmedBlockIndex && other.TxIndex == this.TxIndex;
        }
    }
}
