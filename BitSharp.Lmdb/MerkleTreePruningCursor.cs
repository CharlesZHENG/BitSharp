using BitSharp.Common;
using BitSharp.Core;
using BitSharp.Core.Domain;
using LightningDB;
using System;

namespace BitSharp.Lmdb
{
    internal class MerkleTreePruningCursor : IMerkleTreePruningCursor
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

        public MerkleTreeNode ReadNode()
        {
            var kvPair = cursor.GetCurrent().Value;

            UInt256 recordBlockHash; int txIndex;
            DbEncoder.DecodeBlockHashTxIndex(kvPair.Key, out recordBlockHash, out txIndex);
            if (this.blockHash != recordBlockHash)
                throw new InvalidOperationException();

            var blockTx = DataEncoder.DecodeBlockTx(kvPair.Value, skipTx: true);
            return blockTx;
        }

        public void WriteNode(MerkleTreeNode node)
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
            var blockTx = new BlockTx(node.Index, node.Depth, node.Hash, node.Pruned, null);
            cursor.Put(key, DataEncoder.EncodeBlockTx(blockTx), CursorPutOptions.Current);
        }

        public void DeleteNode()
        {
            cursor.Delete();
            cursor.MovePrev();
        }
    }
}
