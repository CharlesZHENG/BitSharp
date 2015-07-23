using BitSharp.Common;

namespace BitSharp.Core.Domain
{
    public class BlockTx : MerkleTreeNode
    {
        public BlockTx(int index, int depth, UInt256 hash, bool pruned, Transaction transaction)
            : base(index, depth, hash, pruned)
        {
            Transaction = transaction;
        }

        //TODO only used by tests
        public BlockTx(int txIndex, Transaction tx)
            : this(txIndex, 0, tx.Hash, false, tx)
        { }

        public bool IsCoinbase => this.Index == 0;

        public Transaction Transaction { get; }
    }
}
