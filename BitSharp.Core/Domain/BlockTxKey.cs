using BitSharp.Common;

namespace BitSharp.Core.Domain
{
    public class BlockTxKey
    {
        private readonly UInt256 blockHash;
        private readonly int txIndex;

        public BlockTxKey(UInt256 blockHash, int txIndex)
        {
            this.blockHash = blockHash;
            this.txIndex = txIndex;
        }

        public UInt256 BlockHash { get { return this.blockHash; } }

        public int TxIndex { get { return this.txIndex; } }

        public override bool Equals(object obj)
        {
            if (!(obj is BlockTxKey))
                return false;

            var other = (BlockTxKey)obj;
            return other.blockHash == this.blockHash && other.txIndex == this.txIndex;
        }

        public override int GetHashCode()
        {
            return this.blockHash.GetHashCode() ^ this.txIndex.GetHashCode();
        }
    }
}
