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
using Transaction = BitSharp.Core.Domain.Transaction;

namespace BitSharp.Esent
{
    public class BlockTxesStorage : IBlockTxesStorage
    {
        private readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly string jetDirectory;
        private readonly string jetDatabase;
        private readonly Instance jetInstance;

        private readonly DisposableCache<BlockTxesCursor> cursorCache;

        public BlockTxesStorage(string baseDirectory)
        {
            this.jetDirectory = Path.Combine(baseDirectory, "BlockTxes");
            this.jetDatabase = Path.Combine(this.jetDirectory, "BlockTxes.edb");

            this.cursorCache = new DisposableCache<BlockTxesCursor>(64,
                createFunc: () => new BlockTxesCursor(this.jetDatabase, this.jetInstance));

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
            this.cursorCache.Dispose();
            this.jetInstance.Dispose();
        }

        internal Instance JetInstance { get { return this.jetInstance; } }

        public bool ContainsBlock(UInt256 blockHash)
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item;

                using (var jetTx = cursor.jetSession.BeginTransaction())
                {
                    Api.JetSetCurrentIndex(cursor.jetSession, cursor.blocksTableId, "IX_BlockHashTxIndex");
                    Api.MakeKey(cursor.jetSession, cursor.blocksTableId, DbEncoder.EncodeBlockHashTxIndex(blockHash, 0), MakeKeyGrbit.NewKey);

                    if (Api.TrySeek(cursor.jetSession, cursor.blocksTableId, SeekGrbit.SeekGE))
                    {
                        UInt256 recordBlockHash; int txIndex;
                        DbEncoder.DecodeBlockHashTxIndex(Api.RetrieveColumn(cursor.jetSession, cursor.blocksTableId, cursor.blockHashTxIndexColumnId),
                            out recordBlockHash, out txIndex);
                        return blockHash == recordBlockHash;
                    }
                    else
                        return false;
                }
            }
        }

        public void PruneElements(UInt256 blockHash, IEnumerable<int> txIndices)
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item;

                using (var jetTx = cursor.jetSession.BeginTransaction())
                {
                    var pruningCursor = new MerkleTreePruningCursor(blockHash, cursor);
                    //var cachedCursor = new CachedMerkleTreePruningCursor(pruningCursor);

                    // prune the transactions
                    foreach (var index in txIndices)
                        MerkleTree.PruneNode(pruningCursor, index);

                    jetTx.CommitLazy();
                }
            }
        }

        public bool TryReadBlockTransactions(UInt256 blockHash, out IEnumerable<BlockTx> blockTxes)
        {
            if (this.ContainsBlock(blockHash))
            {
                blockTxes = ReadBlockTransactions(blockHash);
                return true;
            }
            else
            {
                blockTxes = null;
                return false;
            }
        }

        private IEnumerable<BlockTx> ReadBlockTransactions(UInt256 blockHash)
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item;

                using (var jetTx = cursor.jetSession.BeginTransaction())
                {
                    Api.JetSetCurrentIndex(cursor.jetSession, cursor.blocksTableId, "IX_BlockHashTxIndex");
                    Api.MakeKey(cursor.jetSession, cursor.blocksTableId, DbEncoder.EncodeBlockHashTxIndex(blockHash, 0), MakeKeyGrbit.NewKey);

                    if (!Api.TrySeek(cursor.jetSession, cursor.blocksTableId, SeekGrbit.SeekGE))
                        yield break;

                    do
                    {
                        UInt256 recordBlockHash; int txIndex;
                        DbEncoder.DecodeBlockHashTxIndex(Api.RetrieveColumn(cursor.jetSession, cursor.blocksTableId, cursor.blockHashTxIndexColumnId),
                            out recordBlockHash, out txIndex);
                        if (blockHash != recordBlockHash)
                            yield break;

                        var depth = Api.RetrieveColumnAsInt32(cursor.jetSession, cursor.blocksTableId, cursor.blockDepthColumnId).Value;
                        var txHash = DbEncoder.DecodeUInt256(Api.RetrieveColumn(cursor.jetSession, cursor.blocksTableId, cursor.blockTxHashColumnId));
                        var txBytes = Api.RetrieveColumn(cursor.jetSession, cursor.blocksTableId, cursor.blockTxBytesColumnId);

                        // determine if transaction is pruned by its depth
                        var pruned = depth >= 0;
                        depth = Math.Max(0, depth);

                        Transaction tx;
                        if (!pruned)
                        {
                            // verify transaction is not corrupt
                            if (txHash != new UInt256(SHA256Static.ComputeDoubleHash(txBytes)))
                                throw new MissingDataException(blockHash);

                            tx = DataEncoder.DecodeTransaction(txBytes, txHash);
                        }
                        else
                        {
                            tx = null;
                        }

                        var blockTx = new BlockTx(txIndex, depth, txHash, pruned, tx);

                        yield return blockTx;
                    }
                    while (Api.TryMoveNext(cursor.jetSession, cursor.blocksTableId));
                }
            }
        }

        private readonly DurationMeasure measure = new DurationMeasure(TimeSpan.FromSeconds(10));
        public bool TryGetTransaction(UInt256 blockHash, int txIndex, out Transaction transaction)
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item;

                using (var jetTx = cursor.jetSession.BeginTransaction())
                {
                    Api.JetSetCurrentIndex(cursor.jetSession, cursor.blocksTableId, "IX_BlockHashTxIndex");
                    Api.MakeKey(cursor.jetSession, cursor.blocksTableId, DbEncoder.EncodeBlockHashTxIndex(blockHash, txIndex), MakeKeyGrbit.NewKey);
                    if (Api.TrySeek(cursor.jetSession, cursor.blocksTableId, SeekGrbit.SeekEQ))
                    {
                        var txBytes = Api.RetrieveColumn(cursor.jetSession, cursor.blocksTableId, cursor.blockTxBytesColumnId);
                        if (txBytes != null)
                        {
                            // verify transaction is not corrupt
                            var txHash = DbEncoder.DecodeUInt256(Api.RetrieveColumn(cursor.jetSession, cursor.blocksTableId, cursor.blockTxHashColumnId));
                            if (txHash != new UInt256(SHA256Static.ComputeDoubleHash(txBytes)))
                                throw new MissingDataException(blockHash);

                            transaction = DataEncoder.DecodeTransaction(txBytes, txHash);
                            return true;
                        }
                        else
                        {
                            transaction = default(Transaction);
                            return false;
                        }
                    }
                    else
                    {
                        transaction = default(Transaction);
                        return false;
                    }
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
            JET_COLUMNID blockCountColumnId;
            JET_COLUMNID flushColumnId;
            JET_TABLEID blocksTableId;
            JET_COLUMNID blockHashTxIndexColumnId;
            JET_COLUMNID blockDepthColumnId;
            JET_COLUMNID blockTxHashColumnId;
            JET_COLUMNID blockTxBytesColumnId;

            using (var jetSession = new Session(this.jetInstance))
            {
                var createGrbit = CreateDatabaseGrbit.None;
                if (EsentVersion.SupportsWindows7Features)
                    createGrbit |= Windows7Grbits.EnableCreateDbBackgroundMaintenance;

                Api.JetCreateDatabase(jetSession, jetDatabase, "", out blockDbId, createGrbit);

                var defaultValue = BitConverter.GetBytes(0);
                Api.JetCreateTable(jetSession, blockDbId, "Globals", 0, 0, out globalsTableId);
                Api.JetAddColumn(jetSession, globalsTableId, "BlockCount", new JET_COLUMNDEF { coltyp = JET_coltyp.Long, grbit = ColumndefGrbit.ColumnEscrowUpdate }, defaultValue, defaultValue.Length, out blockCountColumnId);
                Api.JetAddColumn(jetSession, globalsTableId, "Flush", new JET_COLUMNDEF { coltyp = JET_coltyp.Long, grbit = ColumndefGrbit.ColumnEscrowUpdate }, defaultValue, defaultValue.Length, out flushColumnId);

                // initialize global data
                using (var jetUpdate = jetSession.BeginUpdate(globalsTableId, JET_prep.Insert))
                {
                    Api.SetColumn(jetSession, globalsTableId, blockCountColumnId, 0);
                    Api.SetColumn(jetSession, globalsTableId, flushColumnId, 0);

                    jetUpdate.Save();
                }

                Api.JetCloseTable(jetSession, globalsTableId);

                Api.JetCreateTable(jetSession, blockDbId, "Blocks", 0, 0, out blocksTableId);
                Api.JetAddColumn(jetSession, blocksTableId, "BlockHashTxIndex", new JET_COLUMNDEF { coltyp = JET_coltyp.Binary, cbMax = 36, grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed }, null, 0, out blockHashTxIndexColumnId);
                Api.JetAddColumn(jetSession, blocksTableId, "Depth", new JET_COLUMNDEF { coltyp = JET_coltyp.Long, grbit = ColumndefGrbit.ColumnNotNULL }, null, 0, out blockDepthColumnId);
                Api.JetAddColumn(jetSession, blocksTableId, "TxHash", new JET_COLUMNDEF { coltyp = JET_coltyp.Binary, cbMax = 32, grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed }, null, 0, out blockTxHashColumnId);
                Api.JetAddColumn(jetSession, blocksTableId, "TxBytes", new JET_COLUMNDEF { coltyp = JET_coltyp.LongBinary }, null, 0, out blockTxBytesColumnId);

                Api.JetCreateIndex2(jetSession, blocksTableId,
                    new JET_INDEXCREATE[]
                    {
                        new JET_INDEXCREATE
                        {
                            cbKeyMost = 255,
                            grbit = CreateIndexGrbit.IndexPrimary | CreateIndexGrbit.IndexDisallowNull,
                            szIndexName = "IX_BlockHashTxIndex",
                            szKey = "+BlockHashTxIndex\0\0",
                            cbKey = "+BlockHashTxIndex\0\0".Length
                        }
                    }, 1);

                Api.JetCloseTable(jetSession, blocksTableId);
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
                    JET_DBID blockDbId;
                    Api.JetOpenDatabase(jetSession, this.jetDatabase, "", out blockDbId, readOnly ? OpenDatabaseGrbit.ReadOnly : OpenDatabaseGrbit.None);
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
                        Api.JetCloseDatabase(jetSession, blockDbId, CloseDatabaseGrbit.None);
                        throw;
                    }
                }
                catch (Exception)
                {
                    Api.JetDetachDatabase(jetSession, this.jetDatabase);
                    throw;
                }
            }
        }

        public int BlockCount
        {
            get
            {
                using (var handle = this.cursorCache.TakeItem())
                {
                    var cursor = handle.Item;

                    return Api.RetrieveColumnAsInt32(cursor.jetSession, cursor.globalsTableId, cursor.blockCountColumnId).Value;
                }
            }
        }

        public string Name
        {
            get { return "Blocks"; }
        }

        public bool TryAddBlockTransactions(UInt256 blockHash, IEnumerable<Transaction> blockTxes)
        {
            if (this.ContainsBlock(blockHash))
                return false;

            try
            {
                using (var handle = this.cursorCache.TakeItem())
                {
                    var cursor = handle.Item;

                    using (var jetTx = cursor.jetSession.BeginTransaction())
                    {
                        var txIndex = 0;
                        foreach (var tx in blockTxes)
                        {
                            AddTransaction(blockHash, txIndex, tx.Hash, DataEncoder.EncodeTransaction(tx), cursor);
                            txIndex++;
                        }

                        // increase block count
                        Api.EscrowUpdate(cursor.jetSession, cursor.globalsTableId, cursor.blockCountColumnId, +1);

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

        private void AddTransaction(UInt256 blockHash, int txIndex, UInt256 txHash, byte[] txBytes, BlockTxesCursor cursor)
        {
            using (var jetUpdate = cursor.jetSession.BeginUpdate(cursor.blocksTableId, JET_prep.Insert))
            {
                Api.SetColumn(cursor.jetSession, cursor.blocksTableId, cursor.blockHashTxIndexColumnId, DbEncoder.EncodeBlockHashTxIndex(blockHash, txIndex));
                //TODO i'm using -1 depth to mean not pruned, this should be interpreted as depth 0
                Api.SetColumn(cursor.jetSession, cursor.blocksTableId, cursor.blockDepthColumnId, -1);
                Api.SetColumn(cursor.jetSession, cursor.blocksTableId, cursor.blockTxHashColumnId, DbEncoder.EncodeUInt256(txHash));
                Api.SetColumn(cursor.jetSession, cursor.blocksTableId, cursor.blockTxBytesColumnId, txBytes);

                jetUpdate.Save();
            }
        }

        public bool TryRemoveBlockTransactions(UInt256 blockHash)
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item;

                using (var jetTx = cursor.jetSession.BeginTransaction())
                {
                    // remove transactions
                    Api.JetSetCurrentIndex(cursor.jetSession, cursor.blocksTableId, "IX_BlockHashTxIndex");
                    Api.MakeKey(cursor.jetSession, cursor.blocksTableId, DbEncoder.EncodeBlockHashTxIndex(blockHash, 0), MakeKeyGrbit.NewKey);

                    var removed = false;
                    if (Api.TrySeek(cursor.jetSession, cursor.blocksTableId, SeekGrbit.SeekGE))
                    {
                        do
                        {
                            UInt256 recordBlockHash; int txIndex;
                            DbEncoder.DecodeBlockHashTxIndex(Api.RetrieveColumn(cursor.jetSession, cursor.blocksTableId, cursor.blockHashTxIndexColumnId),
                                out recordBlockHash, out txIndex);
                            if (blockHash == recordBlockHash)
                            {
                                Api.JetDelete(cursor.jetSession, cursor.blocksTableId);
                                removed = true;
                            }
                            else
                            {
                                break;
                            }
                        }
                        while (Api.TryMoveNext(cursor.jetSession, cursor.blocksTableId));
                    }

                    if (removed)
                    {
                        // decrease block count
                        Api.EscrowUpdate(cursor.jetSession, cursor.globalsTableId, cursor.blockCountColumnId, -1);

                        jetTx.CommitLazy();
                        return true;
                    }
                    else
                    {
                        // transactions are already removed
                        return false;
                    }
                }
            }
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
                    this.logger.Info("Begin shrinking block txes database");

                    int actualPages;
                    Windows8Api.JetResizeDatabase(cursor.jetSession, cursor.blockDbId, 0, out actualPages, Windows81Grbits.OnlyShrink);

                    this.logger.Info("Finished shrinking block txes database: {0:#,##0} MB".Format2((float)actualPages * SystemParameters.DatabasePageSize / 1.MILLION()));
                }
            }
        }
    }
}
