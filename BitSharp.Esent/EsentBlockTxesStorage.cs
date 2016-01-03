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
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using Transaction = BitSharp.Core.Domain.Transaction;

namespace BitSharp.Esent
{
    public class EsentBlockTxesStorage : IBlockTxesStorage
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly string jetDirectory;
        private readonly string jetDatabase;
        private readonly Instance jetInstance;

        private readonly DisposableCache<EsentBlockTxesCursor> cursorCache;

        private bool isDisposed;

        public EsentBlockTxesStorage(string baseDirectory, int? index = null)
        {
            this.jetDirectory = Path.Combine(baseDirectory, "BlockTxes");
            if (index.HasValue)
                this.jetDirectory = Path.Combine(jetDirectory, index.Value.ToString());
            this.jetDatabase = Path.Combine(this.jetDirectory, "BlockTxes.edb");

            this.cursorCache = new DisposableCache<EsentBlockTxesCursor>(1024,
                createFunc: () => new EsentBlockTxesCursor(this.jetDatabase, this.jetInstance));

            this.jetInstance = new Instance(Guid.NewGuid().ToString());
            var success = false;
            try
            {
                EsentStorageManager.InitInstanceParameters(jetInstance, jetDirectory);
                this.jetInstance.Init();
                this.CreateOrOpenDatabase();
                success = true;
            }
            finally
            {
                if (!success)
                    this.jetInstance.Dispose();
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

        internal Instance JetInstance => this.jetInstance;

        public bool ContainsBlock(UInt256 blockHash)
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item;

                using (var jetTx = cursor.jetSession.BeginTransaction())
                {
                    Api.JetSetCurrentIndex(cursor.jetSession, cursor.blockIndexTableId, "IX_BlockHash");
                    Api.MakeKey(cursor.jetSession, cursor.blockIndexTableId, DbEncoder.EncodeUInt256(blockHash), MakeKeyGrbit.NewKey);

                    return Api.TrySeek(cursor.jetSession, cursor.blockIndexTableId, SeekGrbit.SeekEQ);
                }
            }
        }

        public void PruneElements(IEnumerable<KeyValuePair<UInt256, IEnumerable<int>>> blockTxIndices)
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item;

                foreach (var keyPair in blockTxIndices)
                {
                    var blockHash = keyPair.Key;
                    var txIndices = keyPair.Value;

                    using (var jetTx = cursor.jetSession.BeginTransaction())
                    {
                        int blockIndex;
                        if (!TryGetBlockIndex(cursor, blockHash, out blockIndex))
                            continue;

                        var pruningCursor = new MerkleTreePruningCursor(blockIndex, cursor);

                        // prune the transactions
                        foreach (var index in txIndices)
                        {
                            var cachedCursor = new CachedMerkleTreePruningCursor<BlockTxNode>(pruningCursor);
                            MerkleTree.PruneNode(cachedCursor, index);
                        }

                        jetTx.CommitLazy();
                    }
                }
            }
        }

        public void DeleteElements(IEnumerable<KeyValuePair<UInt256, IEnumerable<int>>> blockTxIndices)
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item;

                foreach (var keyPair in blockTxIndices)
                {
                    var blockHash = keyPair.Key;
                    var txIndices = keyPair.Value;

                    using (var jetTx = cursor.jetSession.BeginTransaction())
                    {
                        int blockIndex;
                        if (!TryGetBlockIndex(cursor, blockHash, out blockIndex))
                            continue;

                        // prune the transactions
                        foreach (var index in txIndices)
                        {
                            // remove transactions
                            Api.JetSetCurrentIndex(cursor.jetSession, cursor.blocksTableId, "IX_BlockIndexTxIndex");
                            Api.MakeKey(cursor.jetSession, cursor.blocksTableId, blockIndex, MakeKeyGrbit.NewKey);
                            Api.MakeKey(cursor.jetSession, cursor.blocksTableId, index, MakeKeyGrbit.None);

                            if (Api.TrySeek(cursor.jetSession, cursor.blocksTableId, SeekGrbit.SeekEQ))
                                Api.JetDelete(cursor.jetSession, cursor.blocksTableId);
                        }

                        jetTx.CommitLazy();
                    }
                }
            }
        }

        public bool TryReadBlockTransactions(UInt256 blockHash, out IEnumerator<BlockTx> blockTxes)
        {
            if (this.ContainsBlock(blockHash))
            {
                blockTxes = ReadBlockTransactions(blockHash, requireTx: true)
                    .UsingAsEnumerable().Select(x => x.ToBlockTx()).GetEnumerator();
                return true;
            }
            else
            {
                blockTxes = null;
                return false;
            }
        }

        public bool TryReadBlockTxNodes(UInt256 blockHash, out IEnumerator<BlockTxNode> blockTxNodes)
        {
            if (this.ContainsBlock(blockHash))
            {
                blockTxNodes = ReadBlockTransactions(blockHash, requireTx: false);
                return true;
            }
            else
            {
                blockTxNodes = null;
                return false;
            }
        }

        private IEnumerator<BlockTxNode> ReadBlockTransactions(UInt256 blockHash, bool requireTx)
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item;

                using (var jetTx = cursor.jetSession.BeginTransaction())
                {
                    int blockIndex;
                    if (!TryGetBlockIndex(cursor, blockHash, out blockIndex))
                        throw new MissingDataException(blockHash);

                    Api.JetSetCurrentIndex(cursor.jetSession, cursor.blocksTableId, "IX_BlockIndexTxIndex");

                    Api.MakeKey(cursor.jetSession, cursor.blocksTableId, blockIndex, MakeKeyGrbit.NewKey);
                    Api.MakeKey(cursor.jetSession, cursor.blocksTableId, 0, MakeKeyGrbit.None);
                    if (!Api.TrySeek(cursor.jetSession, cursor.blocksTableId, SeekGrbit.SeekGE))
                        throw new MissingDataException(blockHash);

                    Api.MakeKey(cursor.jetSession, cursor.blocksTableId, blockIndex, MakeKeyGrbit.NewKey);
                    Api.MakeKey(cursor.jetSession, cursor.blocksTableId, int.MaxValue, MakeKeyGrbit.None);
                    if (!Api.TrySetIndexRange(cursor.jetSession, cursor.blocksTableId, SetIndexRangeGrbit.RangeUpperLimit))
                        throw new MissingDataException(blockHash);

                    do
                    {
                        var txIndexColumn = new Int32ColumnValue { Columnid = cursor.txIndexColumnId };
                        var blockDepthColumn = new Int32ColumnValue { Columnid = cursor.blockDepthColumnId };
                        var blockTxHashColumn = new BytesColumnValue { Columnid = cursor.blockTxHashColumnId };
                        var blockTxBytesColumn = new BytesColumnValue { Columnid = cursor.blockTxBytesColumnId };
                        Api.RetrieveColumns(cursor.jetSession, cursor.blocksTableId, txIndexColumn, blockDepthColumn, blockTxHashColumn, blockTxBytesColumn);

                        var txIndex = txIndexColumn.Value.Value;
                        var depth = blockDepthColumn.Value.Value;
                        var txHash = DbEncoder.DecodeUInt256(blockTxHashColumn.Value);
                        var txBytes = blockTxBytesColumn.Value;

                        // determine if transaction is pruned by its depth
                        var pruned = depth >= 0;
                        depth = Math.Max(0, depth);

                        if (pruned && requireTx)
                            throw new MissingDataException(blockHash);

                        var blockTxNode = new BlockTxNode(txIndex, depth, txHash, pruned, txBytes?.ToImmutableArray());

                        yield return blockTxNode;
                    }
                    while (Api.TryMoveNext(cursor.jetSession, cursor.blocksTableId));
                }
            }
        }

        public bool TryGetTransaction(UInt256 blockHash, int txIndex, out BlockTx transaction)
        {
            using (var handle = this.cursorCache.TakeItem())
            {
                var cursor = handle.Item;

                using (var jetTx = cursor.jetSession.BeginTransaction())
                {
                    int blockIndex;
                    if (!TryGetBlockIndex(cursor, blockHash, out blockIndex))
                    {
                        transaction = null;
                        return false;
                    }

                    Api.JetSetCurrentIndex(cursor.jetSession, cursor.blocksTableId, "IX_BlockIndexTxIndex");
                    Api.MakeKey(cursor.jetSession, cursor.blocksTableId, blockIndex, MakeKeyGrbit.NewKey);
                    Api.MakeKey(cursor.jetSession, cursor.blocksTableId, txIndex, MakeKeyGrbit.None);
                    if (Api.TrySeek(cursor.jetSession, cursor.blocksTableId, SeekGrbit.SeekEQ))
                    {
                        var blockTxHashColumn = new BytesColumnValue { Columnid = cursor.blockTxHashColumnId };
                        var blockTxBytesColumn = new BytesColumnValue { Columnid = cursor.blockTxBytesColumnId };
                        Api.RetrieveColumns(cursor.jetSession, cursor.blocksTableId, blockTxHashColumn, blockTxBytesColumn);

                        if (blockTxBytesColumn.Value != null)
                        {
                            var txHash = DbEncoder.DecodeUInt256(blockTxHashColumn.Value);
                            transaction = new BlockTx(txIndex, txHash, blockTxBytesColumn.Value.ToImmutableArray());
                            return true;
                        }
                        else
                        {
                            transaction = null;
                            return false;
                        }
                    }
                    else
                    {
                        transaction = null;
                        return false;
                    }
                }
            }
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

            JET_TABLEID blockIndexTableId;
            JET_COLUMNID blockIndexBlockHashColumnId;
            JET_COLUMNID blockIndexBlockIndexColumnId;

            JET_TABLEID blocksTableId;
            JET_COLUMNID blockIndexColumnId;
            JET_COLUMNID blockTxIndexColumnId;
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

                Api.JetCreateTable(jetSession, blockDbId, "BlockIndex", 0, 0, out blockIndexTableId);
                Api.JetAddColumn(jetSession, blockIndexTableId, "BlockHash", new JET_COLUMNDEF { coltyp = JET_coltyp.Binary, cbMax = 32, grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed }, null, 0, out blockIndexBlockHashColumnId);
                Api.JetAddColumn(jetSession, blockIndexTableId, "BlockIndex", new JET_COLUMNDEF { coltyp = JET_coltyp.Long, grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnAutoincrement }, null, 0, out blockIndexBlockIndexColumnId);

                Api.JetCreateIndex2(jetSession, blockIndexTableId,
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

                Api.JetCloseTable(jetSession, blockIndexTableId);

                Api.JetCreateTable(jetSession, blockDbId, "Blocks", 0, 0, out blocksTableId);
                Api.JetAddColumn(jetSession, blocksTableId, "BlockIndex", new JET_COLUMNDEF { coltyp = JET_coltyp.Long, grbit = ColumndefGrbit.ColumnNotNULL }, null, 0, out blockIndexColumnId);
                Api.JetAddColumn(jetSession, blocksTableId, "TxIndex", new JET_COLUMNDEF { coltyp = JET_coltyp.Long, grbit = ColumndefGrbit.ColumnNotNULL }, null, 0, out blockTxIndexColumnId);
                Api.JetAddColumn(jetSession, blocksTableId, "Depth", new JET_COLUMNDEF { coltyp = JET_coltyp.Long, grbit = ColumndefGrbit.ColumnNotNULL }, null, 0, out blockDepthColumnId);
                Api.JetAddColumn(jetSession, blocksTableId, "TxHash", new JET_COLUMNDEF { coltyp = JET_coltyp.Binary, cbMax = 32, grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed }, null, 0, out blockTxHashColumnId);
                Api.JetAddColumn(jetSession, blocksTableId, "TxBytes", new JET_COLUMNDEF { coltyp = JET_coltyp.LongBinary }, null, 0, out blockTxBytesColumnId);

                Api.JetCreateIndex2(jetSession, blocksTableId,
                    new JET_INDEXCREATE[]
                    {
                        new JET_INDEXCREATE
                        {
                            cbKeyMost = 255,
                            grbit = CreateIndexGrbit.IndexUnique | CreateIndexGrbit.IndexDisallowNull,
                            szIndexName = "IX_BlockIndexTxIndex",
                            szKey = "+BlockIndex\0TxIndex\0\0",
                            cbKey = "+BlockIndex\0TxIndex\0\0".Length
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
                var success = false;
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

                        success = true;
                    }
                    finally
                    {
                        if (!success)
                            Api.JetCloseDatabase(jetSession, blockDbId, CloseDatabaseGrbit.None);
                    }
                }
                finally
                {
                    if (!success)
                        Api.JetDetachDatabase(jetSession, this.jetDatabase);
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

        public string Name => "Blocks";

        public bool TryAddBlockTransactions(UInt256 blockHash, IEnumerable<EncodedTx> blockTxes)
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
                        var blockIndex = AddBlockIndex(cursor, blockHash);

                        var txIndex = 0;
                        foreach (var tx in blockTxes)
                        {
                            AddTransaction(blockIndex, txIndex, tx.Hash, tx.TxBytes.ToArray(), cursor);
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

        private void AddTransaction(int blockIndex, int txIndex, UInt256 txHash, byte[] txBytes, EsentBlockTxesCursor cursor)
        {
            using (var jetUpdate = cursor.jetSession.BeginUpdate(cursor.blocksTableId, JET_prep.Insert))
            {
                Api.SetColumns(cursor.jetSession, cursor.blocksTableId,
                    new Int32ColumnValue { Columnid = cursor.blockIndexColumnId, Value = blockIndex },
                    new Int32ColumnValue { Columnid = cursor.txIndexColumnId, Value = txIndex },
                    //TODO i'm using -1 depth to mean not pruned, this should be interpreted as depth 0
                    new Int32ColumnValue { Columnid = cursor.blockDepthColumnId, Value = -1 },
                    new BytesColumnValue { Columnid = cursor.blockTxHashColumnId, Value = DbEncoder.EncodeUInt256(txHash) },
                    new BytesColumnValue { Columnid = cursor.blockTxBytesColumnId, Value = txBytes });

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
                    int blockIndex;
                    if (!TryGetBlockIndex(cursor, blockHash, out blockIndex))
                        return false;

                    DeleteBlockIndex(cursor, blockHash);

                    Api.JetSetCurrentIndex(cursor.jetSession, cursor.blocksTableId, "IX_BlockIndexTxIndex");

                    Api.MakeKey(cursor.jetSession, cursor.blocksTableId, blockIndex, MakeKeyGrbit.NewKey);
                    Api.MakeKey(cursor.jetSession, cursor.blocksTableId, 0, MakeKeyGrbit.None);
                    if (!Api.TrySeek(cursor.jetSession, cursor.blocksTableId, SeekGrbit.SeekGE))
                        return false;

                    Api.MakeKey(cursor.jetSession, cursor.blocksTableId, blockIndex, MakeKeyGrbit.NewKey);
                    Api.MakeKey(cursor.jetSession, cursor.blocksTableId, int.MaxValue, MakeKeyGrbit.None);
                    if (!Api.TrySetIndexRange(cursor.jetSession, cursor.blocksTableId, SetIndexRangeGrbit.RangeUpperLimit))
                        return false;

                    // remove transactions
                    do
                    {
                        Api.JetDelete(cursor.jetSession, cursor.blocksTableId);
                    }
                    while (Api.TryMoveNext(cursor.jetSession, cursor.blocksTableId));

                    // decrease block count
                    Api.EscrowUpdate(cursor.jetSession, cursor.globalsTableId, cursor.blockCountColumnId, -1);

                    jetTx.CommitLazy();
                    return true;
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

                //int passes = int.MaxValue, seconds = int.MaxValue;
                //Api.JetDefragment(cursor.jetSession, cursor.blockDbId, "", ref passes, ref seconds, DefragGrbit.BatchStart);

                if (EsentVersion.SupportsWindows81Features)
                {
                    logger.Info("Begin shrinking block txes database");

                    int actualPages;
                    Windows8Api.JetResizeDatabase(cursor.jetSession, cursor.blockDbId, 0, out actualPages, Windows81Grbits.OnlyShrink);

                    logger.Info($"Finished shrinking block txes database: {(float)actualPages * SystemParameters.DatabasePageSize / 1.MILLION():N0} MB");
                }
            }
        }

        private bool TryGetBlockIndex(EsentBlockTxesCursor cursor, UInt256 blockHash, out int blockIndex)
        {
            Api.JetSetCurrentIndex(cursor.jetSession, cursor.blockIndexTableId, "IX_BlockHash");
            Api.MakeKey(cursor.jetSession, cursor.blockIndexTableId, DbEncoder.EncodeUInt256(blockHash), MakeKeyGrbit.NewKey);

            if (Api.TrySeek(cursor.jetSession, cursor.blockIndexTableId, SeekGrbit.SeekEQ))
            {
                blockIndex = Api.RetrieveColumnAsInt32(cursor.jetSession, cursor.blockIndexTableId, cursor.blockIndexBlockIndexColumnId).Value;
                return true;
            }
            else
            {
                blockIndex = -1;
                return false;
            }
        }

        private int AddBlockIndex(EsentBlockTxesCursor cursor, UInt256 blockHash)
        {
            int blockIndex;
            using (var jetUpdate = cursor.jetSession.BeginUpdate(cursor.blockIndexTableId, JET_prep.Insert))
            {
                Api.SetColumn(cursor.jetSession, cursor.blockIndexTableId, cursor.blockIndexBlockHashColumnId, DbEncoder.EncodeUInt256(blockHash));
                blockIndex = Api.RetrieveColumnAsInt32(cursor.jetSession, cursor.blockIndexTableId, cursor.blockIndexBlockIndexColumnId, RetrieveColumnGrbit.RetrieveCopy).Value;

                jetUpdate.Save();
            }

            return blockIndex;
        }

        private void DeleteBlockIndex(EsentBlockTxesCursor cursor, UInt256 blockHash)
        {
            Api.JetSetCurrentIndex(cursor.jetSession, cursor.blockIndexTableId, "IX_BlockHash");
            Api.MakeKey(cursor.jetSession, cursor.blockIndexTableId, DbEncoder.EncodeUInt256(blockHash), MakeKeyGrbit.NewKey);

            if (Api.TrySeek(cursor.jetSession, cursor.blockIndexTableId, SeekGrbit.SeekEQ))
                Api.JetDelete(cursor.jetSession, cursor.blockIndexTableId);
            else
                throw new InvalidOperationException();
        }
    }
}
