using BitSharp.Common;
using BitSharp.Core;
using BitSharp.Core.Domain;
using LevelDB;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace BitSharp.LevelDb
{
    internal class LevelDbMerkleTreePruningCursor : IMerkleTreePruningCursor<BlockTxNode>
    {
        private readonly UInt256 blockHash;
        private readonly Iterator iterator;
        private readonly Dictionary<int, BlockTxNode> updates = new Dictionary<int, BlockTxNode>();

        public LevelDbMerkleTreePruningCursor(UInt256 blockHash, Iterator iterator)
        {
            this.blockHash = blockHash;
            this.iterator = iterator;
        }

        public bool TryMoveToIndex(int index)
        {
            iterator.Seek(DbEncoder.EncodeBlockHashTxIndex(blockHash, index));
            return iterator.Valid();
        }

        public bool TryMoveLeft()
        {
            return TryMove(forward: false);
        }

        public bool TryMoveRight()
        {
            return TryMove(forward: true);
        }

        public BlockTxNode ReadNode()
        {
            UInt256 recordBlockHash; int txIndex;
            DbEncoder.DecodeBlockHashTxIndex(iterator.Key().ToArray(), out recordBlockHash, out txIndex);
            if (this.blockHash != recordBlockHash)
                throw new InvalidOperationException();

            BlockTxNode blockTxNode;
            if (updates.TryGetValue(txIndex, out blockTxNode))
            {
                if (blockTxNode == null)
                    throw new InvalidOperationException();

                return blockTxNode;
            }
            else
                return DataDecoder.DecodeBlockTxNode(iterator.Value().ToArray(), skipTxBytes: true);
        }

        public void WriteNode(BlockTxNode node)
        {
            if (!node.Pruned)
                throw new ArgumentException();

            UInt256 recordBlockHash; int txIndex;
            DbEncoder.DecodeBlockHashTxIndex(iterator.Key().ToArray(), out recordBlockHash, out txIndex);

            if (this.blockHash != recordBlockHash)
                throw new InvalidOperationException();
            if (node.Index != txIndex)
                throw new InvalidOperationException();

            var blockTxNode = new BlockTxNode(node.Index, node.Depth, node.Hash, node.Pruned, (ImmutableArray<byte>?)null);

            updates[node.Index] = blockTxNode;
        }

        public void DeleteNode()
        {
            UInt256 recordBlockHash; int txIndex;
            DbEncoder.DecodeBlockHashTxIndex(iterator.Key().ToArray(), out recordBlockHash, out txIndex);

            updates[txIndex] = null;

            if (!TryMove(forward: false))
                throw new InvalidOperationException();
        }

        public WriteBatch CreateWriteBatch()
        {
            var writeBatch = new WriteBatch();
            try
            {
                foreach (var update in updates)
                {
                    var txIndex = update.Key;
                    var blockTxNode = update.Value;

                    var key = DbEncoder.EncodeBlockHashTxIndex(blockHash, txIndex);
                    if (blockTxNode != null)
                        writeBatch.Put(key, DataEncoder.EncodeBlockTxNode(blockTxNode));
                    else
                        writeBatch.Delete(key);
                }

                return writeBatch;
            }
            catch (Exception)
            {
                writeBatch.Dispose();
                throw;
            }
        }

        private bool TryMove(bool forward)
        {
            var moveCount = 0;
            while (true)
            {
                if (forward)
                    iterator.Next();
                else
                    iterator.Prev();

                if (iterator.Valid())
                {
                    moveCount++;

                    UInt256 recordBlockHash; int txIndex;
                    DbEncoder.DecodeBlockHashTxIndex(iterator.Key().ToArray(), out recordBlockHash, out txIndex);

                    if (blockHash == recordBlockHash)
                    {
                        BlockTxNode blockTxNode;
                        if (updates.TryGetValue(txIndex, out blockTxNode) && blockTxNode == null)
                            continue;
                        else
                            return true;
                    }
                }
                else if (forward)
                    iterator.SeekToLast();
                else
                    iterator.SeekToFirst();

                for (var i = 0; i < moveCount; i++)
                {
                    if (forward)
                        iterator.Prev();
                    else
                        iterator.Next();
                }
                return false;
            }
        }
    }
}
