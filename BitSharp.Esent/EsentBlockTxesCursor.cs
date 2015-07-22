using Microsoft.Isam.Esent.Interop;
using System;

namespace BitSharp.Esent
{
    internal class EsentBlockTxesCursor : IDisposable
    {
        public readonly string jetDatabase;
        public readonly Instance jetInstance;

        public readonly Session jetSession;
        public readonly JET_DBID blockDbId;

        public readonly JET_TABLEID globalsTableId;
        public readonly JET_COLUMNID blockCountColumnId;
        public readonly JET_COLUMNID flushColumnId;

        public readonly JET_TABLEID blockIndexTableId;
        public readonly JET_COLUMNID blockIndexBlockHashColumnId;
        public readonly JET_COLUMNID blockIndexBlockIndexColumnId;

        public readonly JET_TABLEID blocksTableId;
        public readonly JET_COLUMNID blockIndexColumnId;
        public readonly JET_COLUMNID txIndexColumnId;
        public readonly JET_COLUMNID blockDepthColumnId;
        public readonly JET_COLUMNID blockTxHashColumnId;
        public readonly JET_COLUMNID blockTxBytesColumnId;

        public EsentBlockTxesCursor(string jetDatabase, Instance jetInstance)
        {
            this.jetDatabase = jetDatabase;
            this.jetInstance = jetInstance;

            this.OpenCursor(this.jetDatabase, this.jetInstance, false /*readOnly*/,
                out this.jetSession,
                out this.blockDbId,
                out this.globalsTableId,
                    out this.blockCountColumnId,
                    out this.flushColumnId,
                out this.blockIndexTableId,
                    out this.blockIndexBlockHashColumnId,
                    out this.blockIndexBlockIndexColumnId,
                out this.blocksTableId,
                    out this.blockIndexColumnId,
                    out this.txIndexColumnId,
                    out this.blockDepthColumnId,
                    out this.blockTxHashColumnId,
                    out this.blockTxBytesColumnId);
        }

        public void Dispose()
        {
            this.jetSession.Dispose();
        }

        private void OpenCursor(string jetDatabase, Instance jetInstance, bool readOnly,
            out Session jetSession,
            out JET_DBID blockDbId,
            out JET_TABLEID globalsTableId,
            out JET_COLUMNID blockCountColumnId,
            out JET_COLUMNID flushColumnId,
            out JET_TABLEID blockIndexTableId,
            out JET_COLUMNID blockIndexBlockHashColumnId,
            out JET_COLUMNID blockIndexBlockIndexColumnId,
            out JET_TABLEID blocksTableId,
            out JET_COLUMNID blockIndexColumnId,
            out JET_COLUMNID txIndexColumnId,
            out JET_COLUMNID blockDepthColumnId,
            out JET_COLUMNID blockTxHashColumnId,
            out JET_COLUMNID blockTxBytesColumnId)
        {
            jetSession = new Session(jetInstance);
            var success = false;
            try
            {
                Api.JetOpenDatabase(jetSession, jetDatabase, "", out blockDbId, readOnly ? OpenDatabaseGrbit.ReadOnly : OpenDatabaseGrbit.None);

                Api.JetOpenTable(jetSession, blockDbId, "Globals", null, 0, readOnly ? OpenTableGrbit.ReadOnly : OpenTableGrbit.None, out globalsTableId);
                blockCountColumnId = Api.GetTableColumnid(jetSession, globalsTableId, "BlockCount");
                flushColumnId = Api.GetTableColumnid(jetSession, globalsTableId, "Flush");

                if (!Api.TryMoveFirst(jetSession, globalsTableId))
                    throw new InvalidOperationException();

                Api.JetOpenTable(jetSession, blockDbId, "BlockIndex", null, 0, readOnly ? OpenTableGrbit.ReadOnly : OpenTableGrbit.None, out blockIndexTableId);
                blockIndexBlockHashColumnId = Api.GetTableColumnid(jetSession, blockIndexTableId, "BlockHash");
                blockIndexBlockIndexColumnId = Api.GetTableColumnid(jetSession, blockIndexTableId, "BlockIndex");

                Api.JetOpenTable(jetSession, blockDbId, "Blocks", null, 0, readOnly ? OpenTableGrbit.ReadOnly : OpenTableGrbit.None, out blocksTableId);
                blockIndexColumnId = Api.GetTableColumnid(jetSession, blocksTableId, "BlockIndex");
                txIndexColumnId = Api.GetTableColumnid(jetSession, blocksTableId, "TxIndex");
                blockDepthColumnId = Api.GetTableColumnid(jetSession, blocksTableId, "Depth");
                blockTxHashColumnId = Api.GetTableColumnid(jetSession, blocksTableId, "TxHash");
                blockTxBytesColumnId = Api.GetTableColumnid(jetSession, blocksTableId, "TxBytes");

                success = true;
            }
            finally
            {
                if (!success)
                    jetSession.Dispose();
            }
        }
    }
}
