using BitSharp.Common;
using System.Data.HashFunction;

namespace BitSharp.Core.Domain
{
    /// <summary>
    /// Represents a lookup key to locate a transaction within a block.
    /// </summary>
    public class TxLookupKey
    {
        private readonly int hashCode;

        /// <summary>
        /// Initializes a new instance of <see cref="TxLookupKey"/> with the specified block hash and transaction index.
        /// </summary>
        /// <param name="blockHash">The hash of the block containing the transaction.</param>
        /// <param name="txIndex">The index of the transaction within its block.</param>
        public TxLookupKey(UInt256 blockHash, int txIndex)
        {
            BlockHash = blockHash;
            TxIndex = txIndex;

            var hashBytes = new byte[36];
            blockHash.ToByteArray(hashBytes);
            Bits.EncodeInt32(txIndex, hashBytes, 32);
            hashCode = Bits.ToInt32(new xxHash(32).ComputeHash(hashBytes));
        }

        /// <summary>
        /// Gets the hash of the block containing the transaction.
        /// </summary>
        public UInt256 BlockHash { get; }

        /// <summary>
        /// Gets the index of the transaction within its block.
        /// </summary>
        public int TxIndex { get; }

        public override bool Equals(object obj)
        {
            if (!(obj is TxLookupKey))
                return false;

            var other = (TxLookupKey)obj;
            return other.BlockHash == this.BlockHash && other.TxIndex == this.TxIndex;
        }

        public override int GetHashCode() => hashCode;
    }
}
