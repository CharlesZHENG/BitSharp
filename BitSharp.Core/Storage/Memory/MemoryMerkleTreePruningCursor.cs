using BitSharp.Core.Domain;
using System;
using System.Collections.Generic;

namespace BitSharp.Core.Storage.Memory
{
    public class MemoryMerkleTreePruningCursor<T> : IMerkleTreePruningCursor<T>
        where T : IMerkleTreeNode<T>
    {
        private readonly List<T> nodes;
        private int index;

        public MemoryMerkleTreePruningCursor(IEnumerable<T> nodes)
        {
            this.nodes = new List<T>(nodes);
            this.index = -2;
        }

        public bool TryMoveToIndex(int index)
        {
            this.index = this.nodes.FindIndex(x => x.Index == index);

            if (this.index >= 0 && this.index < this.nodes.Count)
            {
                return true;
            }
            else
            {
                this.index = -2;
                return false;
            }
        }

        public bool TryMoveLeft()
        {
            if (this.index >= 0 && this.index <= this.nodes.Count)
            {
                this.index--;
                if (this.index >= 0 && this.index < this.nodes.Count)
                    return true;
                else
                {
                    this.index++;
                    return false;
                }
            }
            else
            {
                return false;
            }
        }

        public bool TryMoveRight()
        {
            if (this.index >= -1 && this.index < this.nodes.Count)
            {
                this.index++;
                if (this.index >= 0 && this.index < this.nodes.Count)
                    return true;
                else
                {
                    this.index--;
                    return false;
                }
            }
            else
            {
                return false;
            }
        }

        public T ReadNode()
        {
            if (this.index < 0 || this.index >= this.nodes.Count)
                throw new InvalidOperationException();

            return this.nodes[this.index];
        }

        public void WriteNode(T node)
        {
            if (!node.Pruned)
                throw new InvalidOperationException();
            if (this.index < 0 || this.index >= this.nodes.Count)
                throw new InvalidOperationException();

            this.nodes[this.index] = node;
        }

        public void DeleteNode()
        {
            if (this.index < 0 || this.index >= this.nodes.Count)
                throw new InvalidOperationException();

            this.nodes.RemoveAt(this.index);
            this.index--;
        }

        public IEnumerable<T> ReadNodes()
        {
            return this.nodes;
        }
    }
}
