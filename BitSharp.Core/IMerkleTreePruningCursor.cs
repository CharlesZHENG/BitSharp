using BitSharp.Core.Domain;
using System;

namespace BitSharp.Core
{
    public interface IMerkleTreePruningCursor
    {
        bool TryMoveToIndex(int index);

        bool TryMoveLeft();

        bool TryMoveRight();

        MerkleTreeNode ReadNode();

        void WriteNode(MerkleTreeNode node);

        void DeleteNode();
    }

    public static class IMerkleTreePruningCursorExtensionMethods
    {
        public static void MoveToIndex(this IMerkleTreePruningCursor cursor, int index)
        {
            if (!cursor.TryMoveToIndex(index))
                throw new InvalidOperationException();
        }

        public static void MoveLeft(this IMerkleTreePruningCursor cursor)
        {
            if (!cursor.TryMoveLeft())
                throw new InvalidOperationException();
        }

        public static void MoveRight(this IMerkleTreePruningCursor cursor)
        {
            if (!cursor.TryMoveRight())
                throw new InvalidOperationException();
        }
    }
}
