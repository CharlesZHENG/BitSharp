using BitSharp.Core.Domain;
using BitSharp.Core.Storage.Memory;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace BitSharp.Core.Storage
{
    /// <summary>
    /// A pruning cursor which caches reads and movements to a parent pruning cursor.
    /// </summary>
    public class CachedMerkleTreePruningCursor : IMerkleTreePruningCursor
    {
        private readonly IMerkleTreePruningCursor parentCursor;

        private readonly Dictionary<int, MerkleTreeNode> cachedNodes;

        private readonly Dictionary<int, int?> indicesToLeft;
        private readonly Dictionary<int, int?> indicesToRight;

        private int parentCurrentIndex;
        private int currentIndex;
        private bool beforeStart;
        private bool afterEnd;

        public CachedMerkleTreePruningCursor(IMerkleTreePruningCursor parentCursor)
        {
            this.parentCursor = parentCursor;

            this.cachedNodes = new Dictionary<int, MerkleTreeNode>();
            this.indicesToLeft = new Dictionary<int, int?>();
            this.indicesToRight = new Dictionary<int, int?>();

            this.parentCurrentIndex = -1;
            this.currentIndex = -1;
        }

        public bool TryMoveToIndex(int index)
        {
            MerkleTreeNode node;
            if (!this.cachedNodes.TryGetValue(index, out node))
            {
                if (this.parentCurrentIndex != index)
                {
                    if (!this.parentCursor.TryMoveToIndex(index))
                    {
                        this.parentCurrentIndex = -1;
                        this.cachedNodes[index] = null;
                        return false;
                    }

                    this.parentCurrentIndex = index;
                }
                node = this.parentCursor.ReadNode();

                if (node.Index != index)
                    throw new InvalidOperationException();
                this.cachedNodes[index] = node;
            }

            this.currentIndex = (node != null ? index : -1);
            this.beforeStart = false;
            this.afterEnd = false;

            return node != null;
        }

        public bool TryMoveLeft()
        {
            if (this.currentIndex == -1)
                throw new InvalidOperationException();
            else if (this.afterEnd)
            {
                this.afterEnd = false;
                return true;
            }

            int? indexToLeft;
            if (this.indicesToLeft.TryGetValue(this.currentIndex, out indexToLeft))
            {
                if (indexToLeft != null)
                {
                    this.MoveToIndex(indexToLeft.Value);
                    return true;
                }
                else
                {
                    Debug.Assert(this.currentIndex == 0);
                    this.beforeStart = true;
                    return false;
                }
            }
            else
            {
                if (this.parentCurrentIndex != this.currentIndex)
                {
                    this.parentCursor.MoveToIndex(this.currentIndex);
                    this.parentCurrentIndex = this.currentIndex;
                }

                MerkleTreeNode nodeToLeft = null;
                //do
                //{
                if (this.parentCursor.TryMoveLeft())
                    nodeToLeft = this.parentCursor.ReadNode();
                else
                    nodeToLeft = null;
                //}
                //while (nodeToLeft != null); // && this.deletedIndices.Contains(nodeToLeft.Index));

                this.indicesToLeft[this.currentIndex] = (nodeToLeft != null ? nodeToLeft.Index : (int?)null);
                if (nodeToLeft != null)
                {
                    this.indicesToRight[nodeToLeft.Index] = this.currentIndex;
                    this.parentCurrentIndex = nodeToLeft.Index;
                    this.currentIndex = nodeToLeft.Index;
                }
                else
                {
                    this.parentCurrentIndex = -1;
                    this.beforeStart = true;
                }

                return nodeToLeft != null;
            }
        }

        public bool TryMoveRight()
        {
            if (this.currentIndex == -1)
                throw new InvalidOperationException();
            else if (this.beforeStart)
            {
                Debug.Assert(this.currentIndex == 0);
                this.beforeStart = false;
                return true;
            }

            int? indexToRight;
            if (this.indicesToRight.TryGetValue(this.currentIndex, out indexToRight))
            {
                if (indexToRight != null)
                {
                    this.MoveToIndex(indexToRight.Value);
                    return true;
                }
                else
                {
                    this.afterEnd = true;
                    return false;
                }
            }
            else
            {
                if (this.parentCurrentIndex != this.currentIndex)
                {
                    this.parentCursor.MoveToIndex(this.currentIndex);
                    this.parentCurrentIndex = this.currentIndex;
                }

                MerkleTreeNode nodeToRight = null;
                //do
                //{
                if (this.parentCursor.TryMoveRight())
                    nodeToRight = this.parentCursor.ReadNode();
                else
                    nodeToRight = null;
                //}
                //while (nodeToRight != null); // && this.deletedIndices.Contains(nodeToRight.Index));

                this.indicesToRight[this.currentIndex] = (nodeToRight != null ? nodeToRight.Index : (int?)null);
                if (nodeToRight != null)
                {
                    this.indicesToLeft[nodeToRight.Index] = this.currentIndex;
                    this.parentCurrentIndex = nodeToRight.Index;
                    this.currentIndex = nodeToRight.Index;
                }
                else
                {
                    this.parentCurrentIndex = -1;
                    this.afterEnd = true;
                }

                return nodeToRight != null;
            }
        }

        public MerkleTreeNode ReadNode()
        {
            if (this.currentIndex == -1 || this.beforeStart || this.afterEnd)
                throw new InvalidOperationException();

            //if (this.deletedIndices.Contains(this.currentIndex))
            //    throw new InvalidOperationException();

            MerkleTreeNode node;
            if (!this.cachedNodes.TryGetValue(this.currentIndex, out node))
            {
                if (this.parentCurrentIndex != this.currentIndex)
                {
                    this.parentCursor.MoveToIndex(this.currentIndex);
                    this.parentCurrentIndex = this.currentIndex;
                }
                node = this.parentCursor.ReadNode();

                if (node.Index != this.currentIndex)
                    throw new InvalidOperationException();
                this.cachedNodes[this.currentIndex] = node;
            }

            if (node == null)
                throw new InvalidOperationException();

            return node;
        }

        public void WriteNode(MerkleTreeNode node)
        {
            if (this.currentIndex == -1 || this.beforeStart || this.afterEnd)
                throw new InvalidOperationException();
            if (this.currentIndex != node.Index)
                throw new InvalidOperationException();

            if (this.parentCurrentIndex != node.Index)
            {
                this.parentCursor.MoveToIndex(node.Index);
                this.parentCurrentIndex = node.Index;
            }
            this.parentCursor.WriteNode(node);

            this.cachedNodes[node.Index] = node;
        }

        public void DeleteNode()
        {
            if (this.currentIndex == -1 || this.beforeStart || this.afterEnd)
                throw new InvalidOperationException();

            int? nodeToLeftIndex;
            if (this.TryMoveLeft())
            {
                nodeToLeftIndex = this.currentIndex;
            }
            else
            {
                nodeToLeftIndex = null;
            }
            this.MoveRight();

            // try to find the next node to the right, so its index is known
            this.TryMoveRight();
            this.MoveLeft();

            int? nodeToRightIndex;
            if (!this.indicesToRight.TryGetValue(this.currentIndex, out nodeToRightIndex))
                throw new InvalidOperationException();

            if (nodeToLeftIndex != null)
                this.indicesToRight[nodeToLeftIndex.Value] = nodeToRightIndex;
            if (nodeToRightIndex != null)
                this.indicesToLeft[nodeToRightIndex.Value] = nodeToLeftIndex;

            this.cachedNodes.Remove(this.currentIndex);
            this.indicesToLeft.Remove(this.currentIndex);
            this.indicesToRight.Remove(this.currentIndex);

            if (this.parentCurrentIndex != this.currentIndex)
            {
                this.parentCursor.MoveToIndex(this.currentIndex);
                this.parentCurrentIndex = this.currentIndex;
            }
            this.parentCursor.DeleteNode();

            if (nodeToLeftIndex != null)
            {
                this.currentIndex = nodeToLeftIndex.Value;
            }
            else
            {
                Debug.Assert(this.currentIndex == 0);
                this.beforeStart = true;
            }
        }

        //TODO remove
        public IEnumerable<MerkleTreeNode> ReadNodes()
        {
            return ((MemoryMerkleTreePruningCursor)parentCursor).ReadNodes();
        }
    }
}
