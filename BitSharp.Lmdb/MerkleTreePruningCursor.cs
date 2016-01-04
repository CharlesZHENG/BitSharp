using BitSharp.Common;
using BitSharp.Core;
using BitSharp.Core.Domain;
using LightningDB;
using System;
using System.Collections.Immutable;

namespace BitSharp.Lmdb
{
    internal class MerkleTreePruningCursor : IMerkleTreePruningCursor<BlockTxNode>
    {
        private readonly UInt256 blockHash;
        private readonly LightningTransaction txn;
        private readonly LightningDatabase db;
        private readonly LightningCursor cursor;

        public MerkleTreePruningCursor(UInt256 blockHash, LightningTransaction txn, LightningDatabase db, LightningCursor cursor)
        {
            this.blockHash = blockHash;
            this.db = db;
            this.txn = txn;
            this.cursor = cursor;
        }

        public bool TryMoveToIndex(int index)
        {
            return cursor.MoveTo(DbEncoder.EncodeBlockHashTxIndex(blockHash, index)) != null;
        }

        public bool TryMoveLeft()
        {
            var kvPair = cursor.MovePrev();
            if (kvPair != null)
            {
                UInt256 recordBlockHash; int txIndex;
                DbEncoder.DecodeBlockHashTxIndex(kvPair.Value.Key, out recordBlockHash, out txIndex);
                if (blockHash != recordBlockHash)
                    cursor.MoveNext();
                return blockHash == recordBlockHash;
            }
            else
                return false;
        }

        public bool TryMoveRight()
        {
            var kvPair = cursor.MoveNext();
            if (kvPair != null)
            {
                UInt256 recordBlockHash; int txIndex;
                DbEncoder.DecodeBlockHashTxIndex(kvPair.Value.Key, out recordBlockHash, out txIndex);
                if (blockHash != recordBlockHash)
                    cursor.MovePrev();
                return blockHash == recordBlockHash;
            }
            else
                return false;
        }

        public BlockTxNode ReadNode()
        {
            var kvPair = cursor.GetCurrent().Value;

            UInt256 recordBlockHash; int txIndex;
            DbEncoder.DecodeBlockHashTxIndex(kvPair.Key, out recordBlockHash, out txIndex);
            if (this.blockHash != recordBlockHash)
                throw new InvalidOperationException();

            return DataDecoder.DecodeBlockTxNode(kvPair.Value, skipTxBytes: true);
        }

        public void WriteNode(BlockTxNode node)
        {
            if (!node.Pruned)
                throw new ArgumentException();

            var kvPair = cursor.GetCurrent().Value;

            UInt256 recordBlockHash; int txIndex;
            DbEncoder.DecodeBlockHashTxIndex(kvPair.Key, out recordBlockHash, out txIndex);

            if (this.blockHash != recordBlockHash)
                throw new InvalidOperationException();
            if (node.Index != txIndex)
                throw new InvalidOperationException();

            var key = DbEncoder.EncodeBlockHashTxIndex(blockHash, node.Index);
            var blockTxNode = new BlockTxNode(node.Index, node.Depth, node.Hash, node.Pruned, (ImmutableArray<byte>?)null);
            cursor.Put(key, DataEncoder.EncodeBlockTxNode(blockTxNode), CursorPutOptions.Current);
        }

        public void DeleteNode()
        {
            cursor.Delete();
            cursor.MovePrev();
        }
    }
}
