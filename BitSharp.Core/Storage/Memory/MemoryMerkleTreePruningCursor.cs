using BitSharp.Core.Domain;
using System;
using System.Collections.Generic;
using System.Linq;

namespace BitSharp.Core.Storage.Memory
{
    public class MemoryMerkleTreePruningCursor : IMerkleTreePruningCursor
    {
        private readonly List<BlockTx> nodes;
        private int index;

        public MemoryMerkleTreePruningCursor(IEnumerable<MerkleTreeNode> nodes)
        {
            this.nodes = new List<BlockTx>(nodes.Select(x => new BlockTx(x.Index, x.Depth, x.Hash, x.Pruned, null)));
            this.index = -2;
        }

        public MemoryMerkleTreePruningCursor(IEnumerable<BlockTx> nodes)
        {
            this.nodes = new List<BlockTx>(nodes);
            this.index = -2;
        }

        public MemoryMerkleTreePruningCursor(IEnumerable<Transaction> nodes)
        {
            this.nodes = new List<BlockTx>(nodes.Select((tx, txIndex) => new BlockTx(txIndex, 0, tx.Hash, false, tx)));
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

        public MerkleTreeNode ReadNode()
        {
            if (this.index < 0 || this.index >= this.nodes.Count)
                throw new InvalidOperationException();

            return this.nodes[this.index];
        }

        public void WriteNode(MerkleTreeNode node)
        {
            if (!node.Pruned)
                throw new InvalidOperationException();
            if (this.index < 0 || this.index >= this.nodes.Count)
                throw new InvalidOperationException();

            this.nodes[this.index] = new BlockTx(node.Index, node.Depth, node.Hash, node.Pruned, null);
        }

        public void DeleteNode()
        {
            if (this.index < 0 || this.index >= this.nodes.Count)
                throw new InvalidOperationException();

            this.nodes.RemoveAt(this.index);
            this.index--;
        }

        public IEnumerable<BlockTx> ReadNodes()
        {
            return this.nodes;
        }
    }
}
