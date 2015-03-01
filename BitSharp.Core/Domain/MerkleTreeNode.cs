using BitSharp.Common;
using System;

namespace BitSharp.Core.Domain
{
    public class MerkleTreeNode
    {
        private readonly int index;
        private readonly int depth;
        private readonly UInt256 hash;
        private readonly bool pruned;

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

            this.index = index;
            this.depth = depth;
            this.hash = hash;
            this.pruned = pruned;
        }

        public int Index { get { return this.index; } }

        public int Depth { get { return this.depth; } }

        public UInt256 Hash { get { return this.hash; } }

        public bool Pruned { get { return this.pruned; } }

        public bool IsLeft { get { return (this.index >> this.depth) % 2 == 0; } }

        public bool IsRight { get { return !this.IsLeft; } }

        public MerkleTreeNode PairWith(MerkleTreeNode right)
        {
            return Pair(this, right);
        }

        public MerkleTreeNode PairWithSelf()
        {
            return PairWithSelf(this);
        }

        public MerkleTreeNode AsPruned()
        {
            return new MerkleTreeNode(this.index, this.depth, this.hash, pruned: true);
        }

        public override bool Equals(object obj)
        {
            if (!(obj is MerkleTreeNode))
                return false;

            var other = (MerkleTreeNode)obj;
            return other.index == this.index && other.depth == this.depth && other.hash == this.hash && other.pruned == this.pruned;
        }

        public override int GetHashCode()
        {
            return this.index.GetHashCode() ^ this.depth.GetHashCode() ^ this.hash.GetHashCode();
        }

        public static MerkleTreeNode Pair(MerkleTreeNode left, MerkleTreeNode right)
        {
            if (left.Depth != right.Depth)
                throw new InvalidOperationException();
            if (!left.pruned)
                throw new ArgumentException("left");
            if (!right.pruned)
                throw new ArgumentException("right");

            var expectedIndex = left.Index + (1 << left.Depth);
            if (right.Index != expectedIndex)
                throw new InvalidOperationException();

            var pairHashBytes = new byte[64];
            left.Hash.ToByteArray(pairHashBytes, 0);
            right.Hash.ToByteArray(pairHashBytes, 32);

            var pairHash = new UInt256(SHA256Static.ComputeDoubleHash(pairHashBytes));

            return new MerkleTreeNode(left.Index, left.Depth + 1, pairHash, pruned: true);
        }

        public static MerkleTreeNode PairWithSelf(MerkleTreeNode node)
        {
            if (!node.pruned)
                throw new ArgumentException("left");

            return Pair(node, new MerkleTreeNode(node.index + (1 << node.depth), node.depth, node.hash, pruned: true));
        }

        public static bool operator ==(MerkleTreeNode left, MerkleTreeNode right)
        {
            return object.ReferenceEquals(left, right) || (!object.ReferenceEquals(left, null) && !object.ReferenceEquals(right, null) && left.Equals(right));
        }

        public static bool operator !=(MerkleTreeNode left, MerkleTreeNode right)
        {
            return !(left == right);
        }
    }
}
