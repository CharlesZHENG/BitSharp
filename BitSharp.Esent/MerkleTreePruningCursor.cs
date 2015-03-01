using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage;
using BitSharp.Core;
using Microsoft.Isam.Esent.Interop;
using System.IO;
using System.Threading;
using System.Collections.Immutable;
using System.Security.Cryptography;

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
                return blockHash == recordBlockHash;
            }
            else
                return false;
        }

        public bool TryMoveRight()
        {
            if (Api.TryMoveNext(cursor.jetSession, cursor.blocksTableId))
            {
                UInt256 recordBlockHash; int txIndex;
                DbEncoder.DecodeBlockHashTxIndex(Api.RetrieveColumn(cursor.jetSession, cursor.blocksTableId, cursor.blockHashTxIndexColumnId),
                    out recordBlockHash, out txIndex);
                return blockHash == recordBlockHash;
            }
            else
                return false;
        }

        public MerkleTreeNode ReadNode()
        {
            UInt256 recordBlockHash; int txIndex;
            DbEncoder.DecodeBlockHashTxIndex(Api.RetrieveColumn(cursor.jetSession, cursor.blocksTableId, cursor.blockHashTxIndexColumnId),
                out recordBlockHash, out txIndex);
            if (this.blockHash != recordBlockHash)
                throw new InvalidOperationException();

            var depth = Api.RetrieveColumnAsInt32(cursor.jetSession, cursor.blocksTableId, cursor.blockDepthColumnId).Value;
            var txHash = DbEncoder.DecodeUInt256(Api.RetrieveColumn(cursor.jetSession, cursor.blocksTableId, cursor.blockTxHashColumnId));

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
                Api.SetColumn(cursor.jetSession, cursor.blocksTableId, cursor.blockDepthColumnId, node.Depth);
                Api.SetColumn(cursor.jetSession, cursor.blocksTableId, cursor.blockTxHashColumnId, DbEncoder.EncodeUInt256(node.Hash));
                Api.SetColumn(cursor.jetSession, cursor.blocksTableId, cursor.blockTxBytesColumnId, null);

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
