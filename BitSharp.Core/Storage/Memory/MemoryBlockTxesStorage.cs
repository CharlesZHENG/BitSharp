using BitSharp.Common;
using BitSharp.Core.Domain;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Core.Storage.Memory
{
    public class MemoryBlockTxesStorage : IBlockTxesStorage
    {
        private readonly ConcurrentDictionary<UInt256, ImmutableSortedDictionary<int, BlockTxNode>> allBlockTxNodes;

        public MemoryBlockTxesStorage()
        {
            this.allBlockTxNodes = new ConcurrentDictionary<UInt256, ImmutableSortedDictionary<int, BlockTxNode>>();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
        }

        public int BlockCount => this.allBlockTxNodes.Count;

        public bool ContainsBlock(UInt256 blockHash)
        {
            return this.allBlockTxNodes.ContainsKey(blockHash);
        }

        public bool TryAddBlockTransactions(UInt256 blockHash, IEnumerable<EncodedTx> blockTxes)
        {
            return this.allBlockTxNodes.TryAdd(blockHash,
                ImmutableSortedDictionary.CreateRange<int, BlockTxNode>(
                    blockTxes.Select((tx, txIndex) =>
                        new KeyValuePair<int, BlockTxNode>(txIndex, new BlockTx(txIndex, tx)))));
        }

        public bool TryGetTransaction(UInt256 blockHash, int txIndex, out BlockTx transaction)
        {
            ImmutableSortedDictionary<int, BlockTxNode> blockTxes;
            BlockTxNode blockTxNode;

            if (this.allBlockTxNodes.TryGetValue(blockHash, out blockTxes)
                && blockTxes.TryGetValue(txIndex, out blockTxNode)
                && !blockTxNode.Pruned)
            {
                transaction = blockTxNode.ToBlockTx();
                return true;
            }
            else
            {
                transaction = null;
                return false;
            }
        }

        public bool TryRemoveBlockTransactions(UInt256 blockHash)
        {
            ImmutableSortedDictionary<int, BlockTxNode> rawBlockTxNodes;
            return this.allBlockTxNodes.TryRemove(blockHash, out rawBlockTxNodes);
        }

        public bool TryReadBlockTransactions(UInt256 blockHash, out IEnumerator<BlockTx> blockTxes)
        {
            ImmutableSortedDictionary<int, BlockTxNode> rawBlockTxNodes;
            if (this.allBlockTxNodes.TryGetValue(blockHash, out rawBlockTxNodes))
            {
                blockTxes = rawBlockTxNodes.Values.Select(x =>
                {
                    if (!x.Pruned)
                        return x.ToBlockTx();
                    else
                        throw new MissingDataException(blockHash);
                }).GetEnumerator();
                return true;
            }
            else
            {
                blockTxes = null;
                return false;
            }
        }

        public bool TryReadBlockTxNodes(UInt256 blockHash, out IEnumerator<BlockTxNode> blockTxNodes)
        {
            ImmutableSortedDictionary<int, BlockTxNode> rawBlockTxNodes;
            if (this.allBlockTxNodes.TryGetValue(blockHash, out rawBlockTxNodes))
            {
                blockTxNodes = rawBlockTxNodes.Values.GetEnumerator();
                return true;
            }
            else
            {
                blockTxNodes = null;
                return false;
            }
        }

        public void PruneElements(IEnumerable<KeyValuePair<UInt256, IEnumerable<int>>> blockTxIndices)
        {
            foreach (var keyPair in blockTxIndices)
            {
                var blockHash = keyPair.Key;
                var txIndices = keyPair.Value;

                ImmutableSortedDictionary<int, BlockTxNode> blockTxNodes;
                if (this.allBlockTxNodes.TryGetValue(blockHash, out blockTxNodes))
                {
                    var pruningCursor = new MemoryMerkleTreePruningCursor<BlockTxNode>(blockTxNodes.Values);
                    foreach (var index in txIndices)
                        MerkleTree.PruneNode(pruningCursor, index);

                    var prunedBlockTxes =
                        ImmutableSortedDictionary.CreateRange<int, BlockTxNode>(
                            pruningCursor.ReadNodes().Select(blockTx =>
                                new KeyValuePair<int, BlockTxNode>(blockTx.Index, blockTx)));

                    this.allBlockTxNodes[blockHash] = prunedBlockTxes;
                }
            }
        }

        public void DeleteElements(IEnumerable<KeyValuePair<UInt256, IEnumerable<int>>> blockTxIndices)
        {
            foreach (var keyPair in blockTxIndices)
            {
                var blockHash = keyPair.Key;
                var txIndices = keyPair.Value;

                ImmutableSortedDictionary<int, BlockTxNode> blockTxNodes;
                if (this.allBlockTxNodes.TryGetValue(blockHash, out blockTxNodes))
                {
                    var prunedBlockTxes = blockTxNodes.ToBuilder();
                    foreach (var index in txIndices)
                        prunedBlockTxes.Remove(index);

                    this.allBlockTxNodes[blockHash] = prunedBlockTxes.ToImmutable();
                }
            }
        }

        public void Flush()
        {
        }

        public void Defragment()
        {
        }
    }
}
