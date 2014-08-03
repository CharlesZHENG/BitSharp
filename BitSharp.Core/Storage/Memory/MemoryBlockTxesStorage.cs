﻿using BitSharp.Common;
using BitSharp.Core.Domain;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BitSharp.Core.Storage.Memory
{
    public class MemoryBlockTxesStorage : IBlockTxesStorage
    {
        private readonly ConcurrentDictionary<UInt256, ImmutableSortedDictionary<int, BlockTx>> allBlockTxes;

        public MemoryBlockTxesStorage()
        {
            this.allBlockTxes = new ConcurrentDictionary<UInt256, ImmutableSortedDictionary<int, BlockTx>>();
        }

        public void Dispose()
        {
        }

        public int BlockCount
        {
            get { return this.allBlockTxes.Count; }
        }

        public bool ContainsBlock(UInt256 blockHash)
        {
            return this.allBlockTxes.ContainsKey(blockHash);
        }

        public bool TryAddBlockTransactions(UInt256 blockHash, IEnumerable<Transaction> blockTxes)
        {
            return this.allBlockTxes.TryAdd(blockHash,
                ImmutableSortedDictionary.CreateRange<int, BlockTx>(
                    blockTxes.Select((tx, txIndex) =>
                        new KeyValuePair<int, BlockTx>(txIndex, new BlockTx(txIndex, 0, tx.Hash, false, tx)))));
        }

        public bool TryGetTransaction(UInt256 blockHash, int txIndex, out Transaction transaction)
        {
            ImmutableSortedDictionary<int, BlockTx> blockTxes;
            BlockTx blockTx;

            if (this.allBlockTxes.TryGetValue(blockHash, out blockTxes)
                && blockTxes.TryGetValue(txIndex, out blockTx))
            {
                transaction = blockTx.Transaction;
                return true;
            }
            else
            {
                transaction = default(Transaction);
                return false;
            }
        }

        public bool TryRemoveBlockTransactions(UInt256 blockHash)
        {
            ImmutableSortedDictionary<int, BlockTx> blockTxes;
            return this.allBlockTxes.TryRemove(blockHash, out blockTxes);
        }

        public bool TryReadBlockTransactions(UInt256 blockHash, out IEnumerable<BlockTx> blockTxes)
        {
            ImmutableSortedDictionary<int, BlockTx> rawBlockTxes;
            if (this.allBlockTxes.TryGetValue(blockHash, out rawBlockTxes))
            {
                blockTxes = rawBlockTxes.Values;
                return true;
            }
            else
            {
                blockTxes = null;
                return false;
            }
        }

        public void PruneElements(UInt256 blockHash, IEnumerable<int> txIndices)
        {
            ImmutableSortedDictionary<int, BlockTx> blockTxes;
            if (this.allBlockTxes.TryGetValue(blockHash, out blockTxes))
            {
                using (var pruningCursor = new MemoryMerkleTreePruningCursor(blockTxes.Values))
                {
                    foreach (var index in txIndices)
                        MerkleTree.PruneNode(pruningCursor, index);

                    var prunedBlockTxes =
                        ImmutableSortedDictionary.CreateRange<int, BlockTx>(
                            pruningCursor.ReadNodes().Select(blockTx =>
                                new KeyValuePair<int, BlockTx>(blockTx.Index, blockTx)));

                    this.allBlockTxes[blockHash] = prunedBlockTxes;
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
