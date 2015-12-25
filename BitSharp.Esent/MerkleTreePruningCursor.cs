using BitSharp.Core;
using BitSharp.Core.Domain;
using Microsoft.Isam.Esent.Interop;
using System;

namespace BitSharp.Esent
{
    internal class MerkleTreePruningCursor : IMerkleTreePruningCursor<BlockTxNode>
    {
        private readonly int blockIndex;
        private readonly EsentBlockTxesCursor cursor;

        public MerkleTreePruningCursor(int blockIndex, EsentBlockTxesCursor cursor)
        {
            this.blockIndex = blockIndex;
            this.cursor = cursor;
            Api.JetSetCurrentIndex(cursor.jetSession, cursor.blocksTableId, "IX_BlockIndexTxIndex");
        }

        public bool TryMoveToIndex(int index)
        {
            Api.MakeKey(cursor.jetSession, cursor.blocksTableId, blockIndex, MakeKeyGrbit.NewKey);
            Api.MakeKey(cursor.jetSession, cursor.blocksTableId, index, MakeKeyGrbit.None);

            return Api.TrySeek(cursor.jetSession, cursor.blocksTableId, SeekGrbit.SeekEQ);
        }

        public bool TryMoveLeft()
        {
            if (Api.TryMovePrevious(cursor.jetSession, cursor.blocksTableId))
            {
                var recordBlockIndex = Api.RetrieveColumnAsInt32(cursor.jetSession, cursor.blocksTableId, cursor.blockIndexColumnId).Value;

                if (blockIndex != recordBlockIndex)
                    Api.TryMoveNext(cursor.jetSession, cursor.blocksTableId);

                return blockIndex == recordBlockIndex;
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
                var recordBlockIndex = Api.RetrieveColumnAsInt32(cursor.jetSession, cursor.blocksTableId, cursor.blockIndexColumnId).Value;

                if (blockIndex != recordBlockIndex)
                    Api.TryMovePrevious(cursor.jetSession, cursor.blocksTableId);

                return blockIndex == recordBlockIndex;
            }
            else
            {
                Api.TryMovePrevious(cursor.jetSession, cursor.blocksTableId);
                return false;
            }
        }

        public BlockTxNode ReadNode()
        {
            var blockIndexColumn = new Int32ColumnValue { Columnid = cursor.blockIndexColumnId };
            var txIndexColumn = new Int32ColumnValue { Columnid = cursor.txIndexColumnId };
            var depthColumn = new Int32ColumnValue { Columnid = cursor.blockDepthColumnId };
            var txHashColumn = new BytesColumnValue { Columnid = cursor.blockTxHashColumnId };
            Api.RetrieveColumns(cursor.jetSession, cursor.blocksTableId, blockIndexColumn, txIndexColumn, depthColumn, txHashColumn);

            if (this.blockIndex != blockIndexColumn.Value.Value)
                throw new InvalidOperationException();

            var txIndex = txIndexColumn.Value.Value;
            var depth = depthColumn.Value.Value;
            var txHash = DbEncoder.DecodeUInt256(txHashColumn.Value);

            var pruned = depth >= 0;
            depth = Math.Max(0, depth);

            return new BlockTxNode(txIndex, depth, txHash, pruned, encodedTx: null);
        }

        public void WriteNode(BlockTxNode node)
        {
            if (!node.Pruned)
                throw new ArgumentException();

            var recordBlockIndexColumn = new Int32ColumnValue { Columnid = cursor.blockIndexColumnId };
            var recordTxIndexColumn = new Int32ColumnValue { Columnid = cursor.txIndexColumnId };
            Api.RetrieveColumns(cursor.jetSession, cursor.blocksTableId, recordBlockIndexColumn, recordTxIndexColumn);

            if (this.blockIndex != recordBlockIndexColumn.Value.Value)
                throw new InvalidOperationException();
            if (node.Index != recordTxIndexColumn.Value.Value)
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
