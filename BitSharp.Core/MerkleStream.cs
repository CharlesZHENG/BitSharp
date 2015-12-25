using BitSharp.Core.Domain;
using System;
using System.Collections.Generic;
using System.Linq;

namespace BitSharp.Core
{
    public class MerkleStream<T>
        where T : IMerkleTreeNode<T>
    {
        private readonly List<T> leftNodes = new List<T>();
        private int expectedIndex = 0;

        public IMerkleTreeNode<T> RootNode
        {
            get
            {
                if (this.leftNodes.Count != 1)
                    throw new InvalidOperationException();

                return this.leftNodes[0];
            }
        }

        public void AddNode(T newNode)
        {
            // verify index is as expected
            if (newNode.Index != this.expectedIndex)
                throw new InvalidOperationException();

            // determine the index the next node should be
            this.expectedIndex += 1 << newNode.Depth;

            // when streamining nodes, treat them as being pruned so they can be paired together
            newNode = newNode.AsPruned();

            if (this.leftNodes.Count == 0)
            {
                this.leftNodes.Add(newNode);
            }
            else
            {
                var leftNode = this.leftNodes.Last();

                if (newNode.Depth < leftNode.Depth)
                {
                    this.leftNodes.Add(newNode);
                }
                else if (newNode.Depth == leftNode.Depth)
                {
                    this.leftNodes[this.leftNodes.Count - 1] = leftNode.PairWith(newNode);
                }
                else if (newNode.Depth > leftNode.Depth)
                {
                    throw new InvalidOperationException();
                }

                this.ClosePairs();
            }
        }

        public void FinishPairing()
        {
            if (this.leftNodes.Count == 0)
                throw new InvalidOperationException();

            while (this.leftNodes.Count > 1)
            {
                var leftNode = this.leftNodes.Last();
                var rightNode = leftNode.AsPruned(leftNode.Index + (1 << leftNode.Depth), leftNode.Depth, leftNode.Hash);
                AddNode(rightNode);
            }
        }

        private void ClosePairs()
        {
            while (this.leftNodes.Count >= 2)
            {
                var leftNode = this.leftNodes[this.leftNodes.Count - 2];
                var rightNode = this.leftNodes[this.leftNodes.Count - 1];

                if (leftNode.Depth == rightNode.Depth)
                {
                    this.leftNodes.RemoveAt(this.leftNodes.Count - 1);
                    this.leftNodes[this.leftNodes.Count - 1] = leftNode.PairWith(rightNode);
                }
                else
                {
                    break;
                }
            }
        }
    }
}
