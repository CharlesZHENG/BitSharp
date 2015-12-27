using Microsoft.Isam.Esent.Interop;
using Microsoft.Isam.Esent.Interop.Vista;
using Microsoft.Isam.Esent.Interop.Windows7;
using System;

namespace BitSharp.Esent.ChainState
{
    internal static class EsentChainStateSchema
    {
        public static void CreateDatabase(string jetDatabase, Instance jetInstance)
        {
            JET_DBID utxoDbId;

            using (var jetSession = new Session(jetInstance))
            {
                var createGrbit = CreateDatabaseGrbit.None;
                if (EsentVersion.SupportsWindows7Features)
                    createGrbit |= Windows7Grbits.EnableCreateDbBackgroundMaintenance;

                Api.JetCreateDatabase(jetSession, jetDatabase, "", out utxoDbId, createGrbit);

                CreateGlobalsTable(utxoDbId, jetSession);
                CreateFlushTable(utxoDbId, jetSession);
                CreateHeadersTable(utxoDbId, jetSession);
                CreateUnspentTxTable(utxoDbId, jetSession);
                CreateSpentTxTable(utxoDbId, jetSession);
                CreateUnmintedTxTable(utxoDbId, jetSession);
            }
        }

        private static void CreateGlobalsTable(JET_DBID utxoDbId, Session jetSession)
        {
            JET_TABLEID globalsTableId;
            JET_COLUMNID chainTipColumnId;
            JET_COLUMNID unspentTxCountColumnId;
            JET_COLUMNID unspentOutputCountColumnId;
            JET_COLUMNID totalTxCountColumnId;
            JET_COLUMNID totalInputCountColumnId;
            JET_COLUMNID totalOutputCountColumnId;

            Api.JetCreateTable(jetSession, utxoDbId, "Globals", 0, 0, out globalsTableId);
            Api.JetAddColumn(jetSession, globalsTableId, "ChainTip", new JET_COLUMNDEF { coltyp = JET_coltyp.Binary }, null, 0, out chainTipColumnId);
            Api.JetAddColumn(jetSession, globalsTableId, "UnspentTxCount", new JET_COLUMNDEF { coltyp = JET_coltyp.Long, grbit = ColumndefGrbit.ColumnNotNULL }, null, 0, out unspentTxCountColumnId);
            Api.JetAddColumn(jetSession, globalsTableId, "UnspentOutputCount", new JET_COLUMNDEF { coltyp = JET_coltyp.Long, grbit = ColumndefGrbit.ColumnNotNULL }, null, 0, out unspentOutputCountColumnId);
            Api.JetAddColumn(jetSession, globalsTableId, "TotalTxCount", new JET_COLUMNDEF { coltyp = JET_coltyp.Long, grbit = ColumndefGrbit.ColumnNotNULL }, null, 0, out totalTxCountColumnId);
            Api.JetAddColumn(jetSession, globalsTableId, "TotalInputCount", new JET_COLUMNDEF { coltyp = JET_coltyp.Long, grbit = ColumndefGrbit.ColumnNotNULL }, null, 0, out totalInputCountColumnId);
            Api.JetAddColumn(jetSession, globalsTableId, "TotalOutputCount", new JET_COLUMNDEF { coltyp = JET_coltyp.Long, grbit = ColumndefGrbit.ColumnNotNULL }, null, 0, out totalOutputCountColumnId);

            // initialize global data
            using (var jetUpdate = jetSession.BeginUpdate(globalsTableId, JET_prep.Insert))
            {
                Api.SetColumn(jetSession, globalsTableId, unspentTxCountColumnId, 0);
                Api.SetColumn(jetSession, globalsTableId, unspentOutputCountColumnId, 0);
                Api.SetColumn(jetSession, globalsTableId, totalTxCountColumnId, 0);
                Api.SetColumn(jetSession, globalsTableId, totalInputCountColumnId, 0);
                Api.SetColumn(jetSession, globalsTableId, totalOutputCountColumnId, 0);

                jetUpdate.Save();
            }

            Api.JetCloseTable(jetSession, globalsTableId);
        }

        private static void CreateFlushTable(JET_DBID utxoDbId, Session jetSession)
        {
            JET_TABLEID flushTableId;
            JET_COLUMNID flushColumnId;

            var defaultValue = BitConverter.GetBytes(0);

            Api.JetCreateTable(jetSession, utxoDbId, "Flush", 0, 0, out flushTableId);
            Api.JetAddColumn(jetSession, flushTableId, "Flush", new JET_COLUMNDEF { coltyp = JET_coltyp.Long, grbit = ColumndefGrbit.ColumnEscrowUpdate }, defaultValue, defaultValue.Length, out flushColumnId);

            // initialize global data
            using (var jetUpdate = jetSession.BeginUpdate(flushTableId, JET_prep.Insert))
            {
                Api.SetColumn(jetSession, flushTableId, flushColumnId, 0);

                jetUpdate.Save();
            }

            Api.JetCloseTable(jetSession, flushTableId);
        }

        private static void CreateHeadersTable(JET_DBID utxoDbId, Session jetSession)
        {
            JET_TABLEID headersTableId;
            JET_COLUMNID blockHashColumnId;
            JET_COLUMNID headerBytesColumnId;

            Api.JetCreateTable(jetSession, utxoDbId, "Headers", 0, 0, out headersTableId);
            Api.JetAddColumn(jetSession, headersTableId, "BlockHash", new JET_COLUMNDEF { coltyp = JET_coltyp.Binary, cbMax = 32, grbit = ColumndefGrbit.ColumnNotNULL }, null, 0, out blockHashColumnId);
            Api.JetAddColumn(jetSession, headersTableId, "HeaderBytes", new JET_COLUMNDEF { coltyp = JET_coltyp.Binary, grbit = ColumndefGrbit.ColumnNotNULL }, null, 0, out headerBytesColumnId);

            Api.JetCreateIndex2(jetSession, headersTableId,
                new JET_INDEXCREATE[]
                    {
                        new JET_INDEXCREATE
                        {
                            cbKeyMost = 255,
                            grbit = CreateIndexGrbit.IndexUnique | CreateIndexGrbit.IndexDisallowNull,
                            szIndexName = "IX_BlockHash",
                            szKey = "+BlockHash\0\0",
                            cbKey = "+BlockHash\0\0".Length
                        }
                    }, 1);

            Api.JetCloseTable(jetSession, headersTableId);
        }

        private static void CreateUnspentTxTable(JET_DBID utxoDbId, Session jetSession)
        {
            JET_TABLEID unspentTxTableId;
            JET_COLUMNID txHashColumnId;
            JET_COLUMNID blockIndexColumnId;
            JET_COLUMNID txIndexColumnId;
            JET_COLUMNID txVersionColumnId;
            JET_COLUMNID isCoinbaseColumnId;
            JET_COLUMNID outputStatesColumnId;
            JET_COLUMNID txOutputBytesColumnId;

            Api.JetCreateTable(jetSession, utxoDbId, "UnspentTx", 0, 0, out unspentTxTableId);
            Api.JetAddColumn(jetSession, unspentTxTableId, "TxHash", new JET_COLUMNDEF { coltyp = JET_coltyp.Binary, cbMax = 32, grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed }, null, 0, out txHashColumnId);
            Api.JetAddColumn(jetSession, unspentTxTableId, "BlockIndex", new JET_COLUMNDEF { coltyp = JET_coltyp.Long, grbit = ColumndefGrbit.ColumnNotNULL }, null, 0, out blockIndexColumnId);
            Api.JetAddColumn(jetSession, unspentTxTableId, "TxIndex", new JET_COLUMNDEF { coltyp = JET_coltyp.Long, grbit = ColumndefGrbit.ColumnNotNULL }, null, 0, out txIndexColumnId);
            Api.JetAddColumn(jetSession, unspentTxTableId, "TxVersion", new JET_COLUMNDEF { coltyp = VistaColtyp.UnsignedLong, grbit = ColumndefGrbit.ColumnNotNULL }, null, 0, out txVersionColumnId);
            Api.JetAddColumn(jetSession, unspentTxTableId, "IsCoinbase", new JET_COLUMNDEF { coltyp = JET_coltyp.Bit, grbit = ColumndefGrbit.ColumnNotNULL }, null, 0, out isCoinbaseColumnId);
            Api.JetAddColumn(jetSession, unspentTxTableId, "OutputStates", new JET_COLUMNDEF { coltyp = JET_coltyp.LongBinary, grbit = ColumndefGrbit.ColumnNotNULL }, null, 0, out outputStatesColumnId);
            Api.JetAddColumn(jetSession, unspentTxTableId, "TxOutputBytes", new JET_COLUMNDEF { coltyp = JET_coltyp.LongBinary }, null, 0, out txOutputBytesColumnId);

            Api.JetCreateIndex2(jetSession, unspentTxTableId,
                new JET_INDEXCREATE[]
                    {
                        new JET_INDEXCREATE
                        {
                            cbKeyMost = 255,
                            grbit = CreateIndexGrbit.IndexUnique | CreateIndexGrbit.IndexDisallowNull,
                            szIndexName = "IX_TxHash",
                            szKey = "+TxHash\0\0",
                            cbKey = "+TxHash\0\0".Length
                        }
                    }, 1);

            Api.JetCloseTable(jetSession, unspentTxTableId);
        }

        private static void CreateSpentTxTable(JET_DBID utxoDbId, Session jetSession)
        {
            JET_TABLEID spentTxTableId;
            JET_COLUMNID spentSpentBlockIndexColumnId;
            JET_COLUMNID spentDataColumnId;

            Api.JetCreateTable(jetSession, utxoDbId, "SpentTx", 0, 0, out spentTxTableId);
            Api.JetAddColumn(jetSession, spentTxTableId, "SpentBlockIndex", new JET_COLUMNDEF { coltyp = JET_coltyp.Long, grbit = ColumndefGrbit.ColumnNotNULL }, null, 0, out spentSpentBlockIndexColumnId);
            Api.JetAddColumn(jetSession, spentTxTableId, "SpentData", new JET_COLUMNDEF { coltyp = JET_coltyp.LongBinary, grbit = ColumndefGrbit.ColumnNotNULL }, null, 0, out spentDataColumnId);

            Api.JetCreateIndex2(jetSession, spentTxTableId,
                new JET_INDEXCREATE[]
                    {
                        new JET_INDEXCREATE
                        {
                            cbKeyMost = 255,
                            grbit = CreateIndexGrbit.IndexUnique | CreateIndexGrbit.IndexDisallowNull,
                            szIndexName = "IX_SpentBlockIndex",
                            szKey = "+SpentBlockIndex\0\0",
                            cbKey = "+SpentBlockIndex\0\0".Length
                        }
                    }, 1);

            Api.JetCloseTable(jetSession, spentTxTableId);
        }

        private static void CreateUnmintedTxTable(JET_DBID utxoDbId, Session jetSession)
        {
            JET_TABLEID unmintedTxTableId;
            JET_COLUMNID unmintedBlockHashColumnId;
            JET_COLUMNID unmintedDataColumnId;

            Api.JetCreateTable(jetSession, utxoDbId, "UnmintedTx", 0, 0, out unmintedTxTableId);
            Api.JetAddColumn(jetSession, unmintedTxTableId, "BlockHash", new JET_COLUMNDEF { coltyp = JET_coltyp.Binary, cbMax = 32, grbit = ColumndefGrbit.ColumnNotNULL }, null, 0, out unmintedBlockHashColumnId);
            Api.JetAddColumn(jetSession, unmintedTxTableId, "UnmintedData", new JET_COLUMNDEF { coltyp = JET_coltyp.LongBinary, grbit = ColumndefGrbit.ColumnNotNULL }, null, 0, out unmintedDataColumnId);

            Api.JetCreateIndex2(jetSession, unmintedTxTableId,
                new JET_INDEXCREATE[]
                    {
                        new JET_INDEXCREATE
                        {
                            cbKeyMost = 255,
                            grbit = CreateIndexGrbit.IndexUnique | CreateIndexGrbit.IndexDisallowNull,
                            szIndexName = "IX_UnmintedBlockHash",
                            szKey = "+BlockHash\0\0",
                            cbKey = "+BlockHash\0\0".Length
                        }
                    }, 1);

            Api.JetCloseTable(jetSession, unmintedTxTableId);
        }

        public static void OpenDatabase(string jetDatabase, Instance jetInstance, bool readOnly)
        {
            using (var jetSession = new Session(jetInstance))
            {
                var attachGrbit = AttachDatabaseGrbit.None;
                if (readOnly)
                    attachGrbit |= AttachDatabaseGrbit.ReadOnly;
                if (EsentVersion.SupportsWindows7Features)
                    attachGrbit |= Windows7Grbits.EnableAttachDbBackgroundMaintenance;

                Api.JetAttachDatabase(jetSession, jetDatabase, attachGrbit);
                var success = false;
                try
                {
                    using (var cursor = new EsentChainStateCursor(jetDatabase, jetInstance))
                    {
                        // reset flush column
                        using (var jetUpdate = cursor.jetSession.BeginUpdate(cursor.flushTableId, JET_prep.Replace))
                        {
                            Api.SetColumn(cursor.jetSession, cursor.flushTableId, cursor.flushColumnId, 0);

                            jetUpdate.Save();
                        }
                    }

                    success = true;
                }
                finally
                {
                    if (!success)
                        Api.JetDetachDatabase(jetSession, jetDatabase);
                }
            }
        }
    }
}
