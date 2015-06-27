using BitSharp.Common;
using BitSharp.Core;
using BitSharp.Core.Domain;
using Microsoft.Isam.Esent.Interop;
using System;

namespace BitSharp.Esent
{
    internal class MerkleTreePruningCursor : IMerkleTreePruningCursor
    {
        private readonly UInt256 blockHash;
        private readonly BlockTxesCursor cursor;

        public MerkleTreePruningCursor(UInt256 blockHash, BlockTxesCursor cursor)
        {
            this.blockHash = blockHash;
            this.cursor = cursor;
            Api.JetSetCurrentIndex(cursor.jetSession, cursor.blocksTableId, "IX_BlockHashTxIndex");
        }

        public bool TryMoveToIndex(int index)
        {
            Api.MakeKey(cursor.jetSession, cursor.blocksTableId, DbEncoder.EncodeBlockHashTxIndex(blockHash, index), MakeKeyGrbit.NewKey);

            return Api.TrySeek(cursor.jetSession, cursor.blocksTableId, SeekGrbit.SeekEQ);
        }

        public bool TryMoveLeft()
        {
            if (Api.TryMovePrevious(cursor.jetSession, cursor.blocksTableId))
            {
                UInt256 recordBlockHash; int txIndex;
                DbEncoder.DecodeBlockHashTxIndex(Api.RetrieveColumn(cursor.jetSession, cursor.blocksTableId, cursor.blockHashTxIndexColumnId),
                    out recordBlockHash, out txIndex);

                if (blockHash != recordBlockHash)
                    Api.TryMoveNext(cursor.jetSession, cursor.blocksTableId);

                return blockHash == recordBlockHash;
            }
            else
            {
                Api.TryMoveNext(cursor.jetSession, cursor.blocksTableId);
                return false;
            }
        }

        public bool TryMoveRight()
        {
            if (Api.TryMoveNext(cursor.jetSession, cursor.blocksTableId))
            {
                UInt256 recordBlockHash; int txIndex;
                DbEncoder.DecodeBlockHashTxIndex(Api.RetrieveColumn(cursor.jetSession, cursor.blocksTableId, cursor.blockHashTxIndexColumnId),
                    out recordBlockHash, out txIndex);

                if (blockHash != recordBlockHash)
                    Api.TryMovePrevious(cursor.jetSession, cursor.blocksTableId);

                return blockHash == recordBlockHash;
            }
            else
            {
                Api.TryMovePrevious(cursor.jetSession, cursor.blocksTableId);
                return false;
            }
        }

        public MerkleTreeNode ReadNode()
        {
            var blockHashTxIndexColumn = new BytesColumnValue { Columnid = cursor.blockHashTxIndexColumnId };
            var depthColumn = new Int32ColumnValue { Columnid = cursor.blockDepthColumnId };
            var txHashColumn = new BytesColumnValue { Columnid = cursor.blockTxHashColumnId };
            Api.RetrieveColumns(cursor.jetSession, cursor.blocksTableId, blockHashTxIndexColumn, depthColumn, txHashColumn);

            UInt256 recordBlockHash; int txIndex;
            DbEncoder.DecodeBlockHashTxIndex(blockHashTxIndexColumn.Value, out recordBlockHash, out txIndex);
            if (this.blockHash != recordBlockHash)
                throw new InvalidOperationException();

            var depth = depthColumn.Value.Value;
            var txHash = DbEncoder.DecodeUInt256(txHashColumn.Value);

            var pruned = depth >= 0;
            depth = Math.Max(0, depth);

            return new MerkleTreeNode(txIndex, depth, txHash, pruned);
        }

        public void WriteNode(MerkleTreeNode node)
        {
            if (!node.Pruned)
                throw new ArgumentException();

            UInt256 recordBlockHash; int txIndex;
            DbEncoder.DecodeBlockHashTxIndex(Api.RetrieveColumn(cursor.jetSession, cursor.blocksTableId, cursor.blockHashTxIndexColumnId),
                out recordBlockHash, out txIndex);

            if (this.blockHash != recordBlockHash)
                throw new InvalidOperationException();
            if (node.Index != txIndex)
                throw new InvalidOperationException();

            using (var jetUpdate = cursor.jetSession.BeginUpdate(cursor.blocksTableId, JET_prep.Replace))
            {
                Api.SetColumns(cursor.jetSession, cursor.blocksTableId,
                    new Int32ColumnValue { Columnid = cursor.blockDepthColumnId, Value = node.Depth },
                    new BytesColumnValue { Columnid = cursor.blockTxHashColumnId, Value = DbEncoder.EncodeUInt256(node.Hash) },
                    new Int32ColumnValue { Columnid = cursor.blockTxBytesColumnId, Value = null });

                jetUpdate.Save();
            }
        }

        public void DeleteNode()
        {
            Api.JetDelete(cursor.jetSession, cursor.blocksTableId);
            Api.TryMovePrevious(cursor.jetSession, cursor.blocksTableId);
        }
    }
}
