using BitSharp.Common;
using System;

namespace BitSharp.Core.Domain
{
    public class MerkleTreeNode : IMerkleTreeNode<MerkleTreeNode>
    {
        public MerkleTreeNode(int index, int depth, UInt256 hash, bool pruned)
        {
            if (index < 0)
                throw new ArgumentException("index");
            if (depth < 0 || depth > 31)
                throw new ArgumentException("depth");
            if (depth > 0 && !pruned)
                throw new ArgumentException("pruned");

            // ensure no non-zero bits are present in the index below the node's depth
            // i.e. the index is valid for a left or right node at its depth
            if (index % (1 << depth) != 0)
                throw new ArgumentException("depth");

            Index = index;
            Depth = depth;
            Hash = hash;
            Pruned = pruned;
        }

        public int Index { get; }

        public int Depth { get; }

        public UInt256 Hash { get; }

        public bool Pruned { get; }

        public MerkleTreeNode AsPruned()
        {
            return new MerkleTreeNode(Index, Depth, Hash, pruned: true);
        }

        public MerkleTreeNode AsPruned(int index, int depth, UInt256 hash)
        {
            return new MerkleTreeNode(index, depth, hash, pruned: true);
        }

        //TODO only exists for tests
        public override bool Equals(object obj)
        {
            if (!(obj is MerkleTreeNode))
                return false;

            var other = (MerkleTreeNode)obj;
            return other.Index == this.Index && other.Depth == this.Depth && other.Hash == this.Hash && other.Pruned == this.Pruned;
        }
    }
}
