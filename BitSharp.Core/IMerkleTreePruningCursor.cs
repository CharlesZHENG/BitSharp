using BitSharp.Core.Domain;
using System;

namespace BitSharp.Core
{
    public interface IMerkleTreePruningCursor<T>
        where T : IMerkleTreeNode<T>
    {
        bool TryMoveToIndex(int index);

        bool TryMoveLeft();

        bool TryMoveRight();

        T ReadNode();

        void WriteNode(T node);

        void DeleteNode();
    }

    public static class IMerkleTreePruningCursorExtensionMethods
    {
        public static void MoveToIndex<T>(this IMerkleTreePruningCursor<T> cursor, int index)
            where T : IMerkleTreeNode<T>
        {
            if (!cursor.TryMoveToIndex(index))
                throw new InvalidOperationException();
        }

        public static void MoveLeft<T>(this IMerkleTreePruningCursor<T> cursor)
            where T : IMerkleTreeNode<T>
        {
            if (!cursor.TryMoveLeft())
                throw new InvalidOperationException();
        }

        public static void MoveRight<T>(this IMerkleTreePruningCursor<T> cursor)
            where T : IMerkleTreeNode<T>
        {
            if (!cursor.TryMoveRight())
                throw new InvalidOperationException();
        }
    }
}
