using BitSharp.Common;
using System;

namespace BitSharp.Core.Domain
{
    public interface IMerkleTreeNode<T>
    {
        int Index { get; }

        int Depth { get; }

        UInt256 Hash { get; }

        bool Pruned { get; }

        T AsPruned();

        T AsPruned(int index, int depth, UInt256 hash);
    }

    public static class IMerkleTreeNode_ExtensionMethods
    {
        public static bool IsLeft<T>(this IMerkleTreeNode<T> node) => (node.Index >> node.Depth) % 2 == 0;

        public static bool IsRight<T>(this IMerkleTreeNode<T> node) => !node.IsLeft();

        public static T PairWith<T>(this T node, T right)
            where T : IMerkleTreeNode<T>
        {
            return Pair(node, right);
        }

        public static T PairWithSelf<T>(this T node)
            where T : IMerkleTreeNode<T>
        {
            if (!node.Pruned)
                throw new ArgumentException("left");

            var rightNode = node.AsPruned(node.Index + (1 << node.Depth), node.Depth, node.Hash);

            return Pair(node, rightNode);
        }

        public static T Pair<T>(T left, T right)
            where T : IMerkleTreeNode<T>
        {
            if (left.Depth != right.Depth)
                throw new InvalidOperationException();
            if (!left.Pruned)
                throw new ArgumentException("left");
            if (!right.Pruned)
                throw new ArgumentException("right");

            var expectedIndex = left.Index + (1 << left.Depth);
            if (right.Index != expectedIndex)
                throw new InvalidOperationException();

            var pairHashBytes = new byte[64];
            left.Hash.ToByteArray(pairHashBytes, 0);
            right.Hash.ToByteArray(pairHashBytes, 32);

            var pairHash = new UInt256(SHA256Static.ComputeDoubleHash(pairHashBytes));

            return left.AsPruned(left.Index, left.Depth + 1, pairHash);
        }
    }
}
