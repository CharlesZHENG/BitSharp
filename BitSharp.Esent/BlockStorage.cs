using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage;
using Microsoft.Isam.Esent.Interop;
using Microsoft.Isam.Esent.Interop.Server2003;
using Microsoft.Isam.Esent.Interop.Windows7;
using Microsoft.Isam.Esent.Interop.Windows8;
using Microsoft.Isam.Esent.Interop.Windows81;
using NLog;
using System;
using System.Collections.Generic;
using System.IO;

namespace BitSharp.Esent
{
    public class BlockStorage : IBlockStorage
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly string jetDirectory;
        private readonly string jetDatabase;
        private readonly Instance jetInstance;

        private readonly DisposableCache<BlockCursor> cursorCache;

        private bool isDisposed;

        public BlockStorage(string baseDirectory)
        {
            this.jetDirectory = Path.Combine(baseDirectory, "Blocks");
            this.jetDatabase = Path.Combine(this.jetDirectory, "Blocks.edb");

            this.cursorCache = new DisposableCache<BlockCursor>(64,
                createFunc: () => new BlockCursor(this.jetDatabase, this.jetInstance));

            this.jetInstance = CreateInstance(this.jetDirectory);
            try
            {
                this.jetInstance.Init();
                this.CreateOrOpenDatabase();
            }
            catch (Exception)
            {
                this.jetInstance.Dispose();
                throw;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!isDisposed && disposing)
            {
                this.cursorCache.Dispose();
                this.jetInstance.Dispose();

                isDisposed = true;
            }
        }

        public bool ContainsChainedHeader(UInt256 blockHash)
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item;

                Api.JetSetCurrentIndex(cursor.jetSession, cursor.blockHeadersTableId, "IX_BlockHash");
                Api.MakeKey(cursor.jetSession, cursor.blockHeadersTableId, DbEncoder.EncodeUInt256(blockHash), MakeKeyGrbit.NewKey);

                return Api.TrySeek(cursor.jetSession, cursor.blockHeadersTableId, SeekGrbit.SeekEQ);
            }
        }

        public bool TryAddChainedHeader(ChainedHeader chainedHeader)
        {
            try
            {
                using (var handle = this.cursorCache.TakeItem())
                {
                    var cursor = handle.Item;

                    using (var jetTx = cursor.jetSession.BeginTransaction())
                    {
                        using (var jetUpdate = cursor.jetSession.BeginUpdate(cursor.blockHeadersTableId, JET_prep.Insert))
                        {
                            Api.SetColumns(cursor.jetSession, cursor.blockHeadersTableId,
                                new BytesColumnValue { Columnid = cursor.blockHeaderHashColumnId, Value = DbEncoder.EncodeUInt256(chainedHeader.Hash) },
                                new BytesColumnValue { Columnid = cursor.blockHeaderPreviousHashColumnId, Value = DbEncoder.EncodeUInt256(chainedHeader.PreviousBlockHash) },
                                new Int32ColumnValue { Columnid = cursor.blockHeaderHeightColumnId, Value = chainedHeader.Height },
                                new BytesColumnValue { Columnid = cursor.blockHeaderTotalWorkColumnId, Value = DataEncoder.EncodeTotalWork(chainedHeader.TotalWork) },
                                new BytesColumnValue { Columnid = cursor.blockHeaderBytesColumnId, Value = DataEncoder.EncodeChainedHeader(chainedHeader) });

                            jetUpdate.Save();
                        }

                        jetTx.CommitLazy();
                        return true;
                    }
                }
            }
            catch (EsentKeyDuplicateException)
            {
                return false;
            }
        }

        public bool TryGetChainedHeader(UInt256 blockHash, out ChainedHeader chainedHeader)
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item;

                Api.JetSetCurrentIndex(cursor.jetSession, cursor.blockHeadersTableId, "IX_BlockHash");
                Api.MakeKey(cursor.jetSession, cursor.blockHeadersTableId, DbEncoder.EncodeUInt256(blockHash), MakeKeyGrbit.NewKey);
                if (Api.TrySeek(cursor.jetSession, cursor.blockHeadersTableId, SeekGrbit.SeekEQ))
                {
                    chainedHeader = DataEncoder.DecodeChainedHeader(Api.RetrieveColumn(cursor.jetSession, cursor.blockHeadersTableId, cursor.blockHeaderBytesColumnId));
                    return true;
                }
                else
                {
                    chainedHeader = default(ChainedHeader);
                    return false;
                }
            }
        }

        public bool TryRemoveChainedHeader(UInt256 blockHash)
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item;

                using (var jetTx = cursor.jetSession.BeginTransaction())
                {
                    bool removed;

                    Api.JetSetCurrentIndex(cursor.jetSession, cursor.blockHeadersTableId, "IX_BlockHash");
                    Api.MakeKey(cursor.jetSession, cursor.blockHeadersTableId, DbEncoder.EncodeUInt256(blockHash), MakeKeyGrbit.NewKey);
                    if (Api.TrySeek(cursor.jetSession, cursor.blockHeadersTableId, SeekGrbit.SeekEQ))
                    {
                        Api.JetDelete(cursor.jetSession, cursor.blockHeadersTableId);
                        removed = true;
                    }
                    else
                    {
                        removed = false;
                    }

                    jetTx.CommitLazy();
                    return removed;
                }
            }
        }

        public ChainedHeader FindMaxTotalWork()
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item;

                Api.JetSetCurrentIndex(cursor.jetSession, cursor.blockHeadersTableId, "IX_TotalWork");

                // IX_TotalWork is in reverse order, so higher total work comes first
                if (Api.TryMoveFirst(cursor.jetSession, cursor.blockHeadersTableId))
                {
                    do
                    {
                        // check if this block is valid
                        var valid = Api.RetrieveColumnAsBoolean(cursor.jetSession, cursor.blockHeadersTableId, cursor.blockHeaderValidColumnId);
                        if (valid == null || valid.Value)
                        {
                            // decode chained header with most work and return
                            var chainedHeader = DataEncoder.DecodeChainedHeader(Api.RetrieveColumn(cursor.jetSession, cursor.blockHeadersTableId, cursor.blockHeaderBytesColumnId));
                            return chainedHeader;
                        }
                    }
                    while (Api.TryMoveNext(cursor.jetSession, cursor.blockHeadersTableId));
                }

                // no valid chained header found
                return null;
            }
        }

        public IEnumerable<ChainedHeader> ReadChainedHeaders()
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item;

                Api.JetSetCurrentIndex(cursor.jetSession, cursor.blockHeadersTableId, null);

                if (Api.TryMoveFirst(cursor.jetSession, cursor.blockHeadersTableId))
                {
                    do
                    {
                        // decode chained header and return
                        var chainedHeader = DataEncoder.DecodeChainedHeader(Api.RetrieveColumn(cursor.jetSession, cursor.blockHeadersTableId, cursor.blockHeaderBytesColumnId));
                        yield return chainedHeader;
                    }
                    while (Api.TryMoveNext(cursor.jetSession, cursor.blockHeadersTableId));
                }
            }
        }

        public bool IsBlockInvalid(UInt256 blockHash)
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item;

                Api.JetSetCurrentIndex(cursor.jetSession, cursor.blockHeadersTableId, "IX_BlockHash");
                Api.MakeKey(cursor.jetSession, cursor.blockHeadersTableId, DbEncoder.EncodeUInt256(blockHash), MakeKeyGrbit.NewKey);

                if (Api.TrySeek(cursor.jetSession, cursor.blockHeadersTableId, SeekGrbit.SeekEQ))
                {
                    var valid = Api.RetrieveColumnAsBoolean(cursor.jetSession, cursor.blockHeadersTableId, cursor.blockHeaderValidColumnId)
                        ?? true;
                    return !valid;
                }
                else
                {
                    return false;
                }
            }
        }

        public void MarkBlockInvalid(UInt256 blockHash)
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item;

                using (var jetTx = cursor.jetSession.BeginTransaction())
                {
                    Api.JetSetCurrentIndex(cursor.jetSession, cursor.blockHeadersTableId, "IX_BlockHash");
                    Api.MakeKey(cursor.jetSession, cursor.blockHeadersTableId, DbEncoder.EncodeUInt256(blockHash), MakeKeyGrbit.NewKey);

                    if (!Api.TrySeek(cursor.jetSession, cursor.blockHeadersTableId, SeekGrbit.SeekEQ))
                        throw new MissingDataException(blockHash);

                    using (var jetUpdate = cursor.jetSession.BeginUpdate(cursor.blockHeadersTableId, JET_prep.Replace))
                    {
                        Api.SetColumn(cursor.jetSession, cursor.blockHeadersTableId, cursor.blockHeaderValidColumnId, false);

                        jetUpdate.Save();
                    }

                    jetTx.CommitLazy();
                }
            }
        }

        private static Instance CreateInstance(string directory)
        {
            var instance = new Instance(Guid.NewGuid().ToString());

            instance.Parameters.SystemDirectory = directory;
            instance.Parameters.LogFileDirectory = directory;
            instance.Parameters.TempDirectory = directory;
            instance.Parameters.AlternateDatabaseRecoveryDirectory = directory;
            instance.Parameters.CreatePathIfNotExist = true;
            instance.Parameters.BaseName = "epc";
            instance.Parameters.EnableIndexChecking = false;
            instance.Parameters.CircularLog = true;
            instance.Parameters.CheckpointDepthMax = 64 * 1024 * 1024;
            instance.Parameters.LogFileSize = 1024;
            instance.Parameters.LogBuffers = 1024;
            instance.Parameters.CleanupMismatchedLogFiles = true;
            instance.Parameters.MaxTemporaryTables = 16;
            instance.Parameters.MaxVerPages = 1024 * 256;
            instance.Parameters.NoInformationEvent = true;
            instance.Parameters.WaypointLatency = 1;
            instance.Parameters.MaxSessions = 256;
            instance.Parameters.MaxOpenTables = 256;
            if (EsentVersion.SupportsWindows81Features)
            {
                instance.Parameters.EnableShrinkDatabase = ShrinkDatabaseGrbit.On | ShrinkDatabaseGrbit.Realtime;
            }

            return instance;
        }

        private void CreateOrOpenDatabase()
        {
            try
            {
                OpenDatabase();
            }
            catch (Exception)
            {
                DeleteDatabase();
                CreateDatabase();
            }
        }

        private void CreateDatabase()
        {
            JET_DBID blockDbId;
            JET_TABLEID globalsTableId;
            JET_COLUMNID flushColumnId;
            JET_TABLEID blockHeadersTableId;
            JET_COLUMNID blockHeaderHashColumnId;
            JET_COLUMNID blockHeaderPreviousHashColumnId;
            JET_COLUMNID blockHeaderHeightColumnId;
            JET_COLUMNID blockHeaderTotalWorkColumnId;
            JET_COLUMNID blockHeaderValidColumnId;
            JET_COLUMNID blockHeaderBytesColumnId;

            using (var jetSession = new Session(this.jetInstance))
            {
                var createGrbit = CreateDatabaseGrbit.None;
                if (EsentVersion.SupportsWindows7Features)
                    createGrbit |= Windows7Grbits.EnableCreateDbBackgroundMaintenance;

                Api.JetCreateDatabase(jetSession, jetDatabase, "", out blockDbId, createGrbit);

                var defaultValue = BitConverter.GetBytes(0);
                Api.JetCreateTable(jetSession, blockDbId, "Globals", 0, 0, out globalsTableId);
                Api.JetAddColumn(jetSession, globalsTableId, "Flush", new JET_COLUMNDEF { coltyp = JET_coltyp.Long, grbit = ColumndefGrbit.ColumnEscrowUpdate }, defaultValue, defaultValue.Length, out flushColumnId);

                // initialize global data
                using (var jetUpdate = jetSession.BeginUpdate(globalsTableId, JET_prep.Insert))
                {
                    Api.SetColumn(jetSession, globalsTableId, flushColumnId, 0);

                    jetUpdate.Save();
                }

                Api.JetCloseTable(jetSession, globalsTableId);

                Api.JetCreateTable(jetSession, blockDbId, "BlockHeaders", 0, 0, out blockHeadersTableId);
                Api.JetAddColumn(jetSession, blockHeadersTableId, "BlockHash", new JET_COLUMNDEF { coltyp = JET_coltyp.Binary, cbMax = 32, grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed }, null, 0, out blockHeaderHashColumnId);
                Api.JetAddColumn(jetSession, blockHeadersTableId, "PreviousBlockHash", new JET_COLUMNDEF { coltyp = JET_coltyp.Binary, cbMax = 32, grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed }, null, 0, out blockHeaderPreviousHashColumnId);
                Api.JetAddColumn(jetSession, blockHeadersTableId, "Height", new JET_COLUMNDEF { coltyp = JET_coltyp.Long, grbit = ColumndefGrbit.ColumnNotNULL }, null, 0, out blockHeaderHeightColumnId);
                Api.JetAddColumn(jetSession, blockHeadersTableId, "TotalWork", new JET_COLUMNDEF { coltyp = JET_coltyp.Binary, grbit = ColumndefGrbit.ColumnNotNULL }, null, 0, out blockHeaderTotalWorkColumnId);
                Api.JetAddColumn(jetSession, blockHeadersTableId, "Valid", new JET_COLUMNDEF { coltyp = JET_coltyp.Bit, }, null, 0, out blockHeaderValidColumnId);
                Api.JetAddColumn(jetSession, blockHeadersTableId, "BlockHeaderBytes", new JET_COLUMNDEF { coltyp = JET_coltyp.Binary, grbit = ColumndefGrbit.ColumnNotNULL }, null, 0, out blockHeaderBytesColumnId);

                Api.JetCreateIndex2(jetSession, blockHeadersTableId,
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

                //Api.JetCreateIndex2(jetSession, blockHeadersTableId,
                //    new JET_INDEXCREATE[]
                //    {
                //        new JET_INDEXCREATE
                //        {
                //            cbKeyMost = 255,
                //            grbit = CreateIndexGrbit.IndexDisallowNull,
                //            szIndexName = "IX_PreviousBlockHash",
                //            szKey = "+PreviousBlockHash\0\0",
                //            cbKey = "+PreviousBlockHash\0\0".Length
                //        }
                //    }, 1);

                //Api.JetCreateIndex2(jetSession, blockHeadersTableId,
                //    new JET_INDEXCREATE[]
                //    {
                //        new JET_INDEXCREATE
                //        {
                //            cbKeyMost = 255,
                //            grbit = CreateIndexGrbit.IndexDisallowNull,
                //            szIndexName = "IX_Height",
                //            szKey = "+Height\0\0",
                //            cbKey = "+Height\0\0".Length
                //        }
                //    }, 1);

                Api.JetCreateIndex2(jetSession, blockHeadersTableId,
                    new JET_INDEXCREATE[]
                    {
                        new JET_INDEXCREATE
                        {
                            cbKeyMost = 255,
                            grbit = CreateIndexGrbit.IndexDisallowNull,
                            szIndexName = "IX_TotalWork",
                            szKey = "-TotalWork\0\0",
                            cbKey = "-TotalWork\0\0".Length
                        }
                    }, 1);

                Api.JetCloseTable(jetSession, blockHeadersTableId);
            }
        }

        private void DeleteDatabase()
        {
            try { Directory.Delete(this.jetDirectory, recursive: true); }
            catch (Exception) { }
            Directory.CreateDirectory(this.jetDirectory);
        }

        private void OpenDatabase()
        {
            var readOnly = false;

            using (var jetSession = new Session(this.jetInstance))
            {
                var attachGrbit = AttachDatabaseGrbit.None;
                if (readOnly)
                    attachGrbit |= AttachDatabaseGrbit.ReadOnly;
                if (EsentVersion.SupportsWindows7Features)
                    attachGrbit |= Windows7Grbits.EnableAttachDbBackgroundMaintenance;

                Api.JetAttachDatabase(jetSession, this.jetDatabase, attachGrbit);
                try
                {
                    using (var handle = this.cursorCache.TakeItem())
                    {
                        var cursor = handle.Item;

                        // reset flush column
                        using (var jetUpdate = cursor.jetSession.BeginUpdate(cursor.globalsTableId, JET_prep.Replace))
                        {
                            Api.SetColumn(cursor.jetSession, cursor.globalsTableId, cursor.flushColumnId, 0);

                            jetUpdate.Save();
                        }
                    }
                }
                catch (Exception)
                {
                    Api.JetDetachDatabase(jetSession, jetDatabase);
                    throw;
                }
            }
        }

        public int Count
        {
            get { return 0; }
        }

        public string Name
        {
            get { return "Blocks"; }
        }

        public bool TryRemove(UInt256 blockHash)
        {
            throw new NotImplementedException();
        }

        public void Flush()
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item;

                if (EsentVersion.SupportsServer2003Features)
                {
                    Api.JetCommitTransaction(cursor.jetSession, Server2003Grbits.WaitAllLevel0Commit);
                }
                else
                {
                    using (var jetTx = cursor.jetSession.BeginTransaction())
                    {
                        Api.EscrowUpdate(cursor.jetSession, cursor.globalsTableId, cursor.flushColumnId, 1);
                        jetTx.Commit(CommitTransactionGrbit.None);
                    }
                }
            }
        }

        public void Defragment()
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item;

                //int passes = -1, seconds = -1;
                //Api.JetDefragment(cursor.jetSession, cursor.blockDbId, "BlockTxes", ref passes, ref seconds, DefragGrbit.BatchStart);

                if (EsentVersion.SupportsWindows81Features)
                {
                    logger.Info("Begin shrinking block database");

                    int actualPages;
                    Windows8Api.JetResizeDatabase(cursor.jetSession, cursor.blockDbId, 0, out actualPages, Windows81Grbits.OnlyShrink);

                    logger.Info("Finished shrinking block database: {0:#,##0} MB".Format2((float)actualPages * SystemParameters.DatabasePageSize / 1.MILLION()));
                }
            }
        }
    }
}
