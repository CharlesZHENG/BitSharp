using BitSharp.Common;

namespace BitSharp.Core.Domain
{
    public class BlockTx : MerkleTreeNode
    {
        private readonly Transaction transaction;

        public BlockTx(int index, int depth, UInt256 hash, bool pruned, Transaction transaction)
            : base(index, depth, hash, pruned)
        {
            this.transaction = transaction;
        }

        public Transaction Transaction { get { return this.transaction; } }
    }
}
