using BitSharp.Core.Domain;
using NLog;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace BitSharp.Core.Storage
{
    /// <summary>
    /// A pruning cursor which caches reads and movements to a parent pruning cursor.
    /// </summary>
    public class CachedMerkleTreePruningCursor<T> : IMerkleTreePruningCursor<T>
        where T : class, IMerkleTreeNode<T>
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly ParentCursor parentCursor;

        private readonly Dictionary<int, T> cachedNodes;

        private readonly Dictionary<int, int?> indicesToLeft;
        private readonly Dictionary<int, int?> indicesToRight;

        private int currentIndex;

        public CachedMerkleTreePruningCursor(IMerkleTreePruningCursor<T> parentCursor)
        {
            this.parentCursor = new ParentCursor(parentCursor);

            cachedNodes = new Dictionary<int, T>();
            indicesToLeft = new Dictionary<int, int?>();
            indicesToRight = new Dictionary<int, int?>();

            currentIndex = -1;
        }

        public bool TryMoveToIndex(int index)
        {
            T node;
            if (cachedNodes.TryGetValue(index, out node))
            {
                currentIndex = index;
                return true;
            }
            else if (parentCursor.TryMoveToIndex(index))
            {
                node = parentCursor.ReadNode(index);
                cachedNodes[index] = node;
                currentIndex = node.Index;
                return true;
            }
            else
            {
                cachedNodes[index] = null;
                currentIndex = -1;
                return false;
            }
        }

        public bool TryMoveLeft()
        {
            if (currentIndex == -1)
                throw new InvalidOperationException();

            int? indexToLeft;
            if (indicesToLeft.TryGetValue(currentIndex, out indexToLeft))
            {
                if (indexToLeft != null)
                    currentIndex = indexToLeft.Value;
                return indexToLeft.HasValue;
            }
            else
            {
                parentCursor.MoveToIndex(currentIndex, indicesToLeft, indicesToRight);

                T leftNode;
                if (parentCursor.TryMoveLeft(out leftNode))
                {
                    indicesToLeft[currentIndex] = leftNode.Index;
                    indicesToRight[leftNode.Index] = currentIndex;
                    cachedNodes[leftNode.Index] = leftNode;

                    currentIndex = leftNode.Index;
                    return true;
                }
                else
                {
                    indicesToLeft[currentIndex] = null;
                    return false;
                }
            }
        }

        public bool TryMoveRight()
        {
            if (currentIndex == -1)
                throw new InvalidOperationException();

            int? indexToRight;
            if (indicesToRight.TryGetValue(currentIndex, out indexToRight))
            {
                if (indexToRight != null)
                    currentIndex = indexToRight.Value;
                return indexToRight.HasValue;
            }
            else
            {
                parentCursor.MoveToIndex(currentIndex, indicesToLeft, indicesToRight);

                T rightNode;
                if (parentCursor.TryMoveRight(out rightNode))
                {
                    indicesToRight[currentIndex] = rightNode.Index;
                    indicesToLeft[rightNode.Index] = currentIndex;
                    cachedNodes[rightNode.Index] = rightNode;

                    currentIndex = rightNode.Index;
                    return true;
                }
                else
                {
                    indicesToRight[currentIndex] = null;
                    return false;
                }
            }
        }

        public T ReadNode()
        {
            if (currentIndex == -1)
                throw new InvalidOperationException();

            T node;
            if (!cachedNodes.TryGetValue(currentIndex, out node) || node == null)
                throw new InvalidOperationException();

            return node;
        }

        public void WriteNode(T node)
        {
            if (currentIndex == -1)
                throw new InvalidOperationException();
            if (currentIndex != node.Index)
                throw new InvalidOperationException();

            parentCursor.MoveToIndex(node.Index, indicesToLeft, indicesToRight);
            parentCursor.WriteNode(node);

            cachedNodes[node.Index] = node;
        }

        public void DeleteNode()
        {
            if (currentIndex == -1)
                throw new InvalidOperationException();

            // get the index of the node to the left
            // a node to the left will always exist, tx index 0 will never be deleted by pruning
            this.MoveLeft();
            var nodeToLeftIndex = currentIndex;
            this.MoveRight();
            Debug.Assert(nodeToLeftIndex < currentIndex);

            // try to find the next node to the right, so its index is known
            if (TryMoveRight())
                this.MoveLeft();
            Debug.Assert(nodeToLeftIndex < currentIndex);

            // delete on the parent cursor
            parentCursor.MoveToIndex(currentIndex, indicesToLeft, indicesToRight);
            parentCursor.DeleteNode(currentIndex, nodeToLeftIndex);

            // update cached cursor indices
            int? nodeToRightIndex;
            if (!indicesToRight.TryGetValue(currentIndex, out nodeToRightIndex))
                throw new InvalidOperationException();

            indicesToRight[nodeToLeftIndex] = nodeToRightIndex;
            if (nodeToRightIndex != null)
            {
                Debug.Assert(nodeToRightIndex.Value > currentIndex);
                indicesToLeft[nodeToRightIndex.Value] = nodeToLeftIndex;
            }

            // clear cached indices of deleted node
            cachedNodes.Remove(currentIndex);
            indicesToLeft.Remove(currentIndex);
            indicesToRight.Remove(currentIndex);

            currentIndex = nodeToLeftIndex;
        }

        private sealed class ParentCursor
        {
            private readonly IMerkleTreePruningCursor<T> pruningCursor;
            private int currentIndex;

            public ParentCursor(IMerkleTreePruningCursor<T> pruningCursor)
            {
                this.pruningCursor = pruningCursor;
                currentIndex = -1;
            }

            public T ReadNode(int expectedIndex)
            {
                if (expectedIndex != currentIndex)
                    throw new InvalidOperationException();

                var node = pruningCursor.ReadNode();

                if (node.Index != expectedIndex)
                    throw new InvalidOperationException();

                return node;
            }

            public bool TryMoveToIndex(int index)
            {
                var result = pruningCursor.TryMoveToIndex(index);
                currentIndex = result ? index : -1;
                return result;
            }

            public void MoveToIndex(int index, Dictionary<int, int?> indicesToLeft, Dictionary<int, int?> indicesToRight)
            {
                if (currentIndex == -1)
                {
                    pruningCursor.MoveToIndex(index);
                    currentIndex = index;
                    return;
                }

                if (currentIndex > index)
                {
                    int? leftIndex;
                    while (indicesToLeft.TryGetValue(currentIndex, out leftIndex))
                    {
                        if (!leftIndex.HasValue)
                            throw new InvalidOperationException();

                        pruningCursor.MoveLeft();
                        currentIndex = leftIndex.Value;

                        if (currentIndex == index)
                            return;
                        else if (currentIndex < index)
                            break;
                    }
                }
                else if (currentIndex < index)
                {
                    int? rightIndex;
                    while (indicesToRight.TryGetValue(currentIndex, out rightIndex))
                    {
                        if (!rightIndex.HasValue)
                            throw new InvalidOperationException();

                        pruningCursor.MoveRight();
                        currentIndex = rightIndex.Value;

                        if (currentIndex == index)
                            return;
                        else if (currentIndex > index)
                            break;
                    }
                }

                if (currentIndex != index)
                {
                    pruningCursor.MoveToIndex(index);
                    currentIndex = index;
                }

                Debug.Assert(pruningCursor.ReadNode().Index == index);
            }

            public bool TryMoveLeft(out T leftNode)
            {
                if (pruningCursor.TryMoveLeft())
                {
                    leftNode = pruningCursor.ReadNode();
                    currentIndex = leftNode.Index;
                    return true;
                }
                else
                {
                    leftNode = null;
                    return false;
                }
            }

            public bool TryMoveRight(out T rightNode)
            {
                if (pruningCursor.TryMoveRight())
                {
                    rightNode = pruningCursor.ReadNode();
                    currentIndex = rightNode.Index;
                    return true;
                }
                else
                {
                    rightNode = null;
                    return false;
                }
            }

            public void WriteNode(T node)
            {
                if (node.Index != currentIndex)
                    throw new InvalidOperationException();

                pruningCursor.WriteNode(node);
            }

            public void DeleteNode(int expectedIndex, int nodeToLeftIndex)
            {
                if (expectedIndex != currentIndex)
                    throw new InvalidOperationException();

                pruningCursor.DeleteNode();
                currentIndex = nodeToLeftIndex;

                Debug.Assert(pruningCursor.ReadNode().Index == nodeToLeftIndex);
            }
        }
    }
}
