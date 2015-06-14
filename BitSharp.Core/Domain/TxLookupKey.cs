using BitSharp.Common;

namespace BitSharp.Core.Domain
{
    /// <summary>
    /// Represents a lookup key to locate a transaction within a block.
    /// </summary>
    public class TxLookupKey
    {
        private readonly UInt256 blockHash;
        private readonly int txIndex;

        /// <summary>
        /// Initializes a new instance of <see cref="TxLookupKey"/> with the specified block hash and transaction index.
        /// </summary>
        /// <param name="blockHash">The hash of the block containing the transaction.</param>
        /// <param name="txIndex">The index of the transaction within its block.</param>
        public TxLookupKey(UInt256 blockHash, int txIndex)
        {
            this.blockHash = blockHash;
            this.txIndex = txIndex;
        }

        /// <summary>
        /// Gets the hash of the block containing the transaction.
        /// </summary>
        public UInt256 BlockHash { get { return this.blockHash; } }

        /// <summary>
        /// Gets the index of the transaction within its block.
        /// </summary>
        public int TxIndex { get { return this.txIndex; } }

        public override bool Equals(object obj)
        {
            if (!(obj is TxLookupKey))
                return false;

            var other = (TxLookupKey)obj;
            return other.blockHash == this.blockHash && other.txIndex == this.txIndex;
        }

        public override int GetHashCode()
        {
            return this.blockHash.GetHashCode() ^ this.txIndex.GetHashCode();
        }
    }
}
