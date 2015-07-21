using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core;
using BitSharp.Core.Domain;
using BitSharp.Core.ExtensionMethods;
using BitSharp.Core.Storage;
using Microsoft.Isam.Esent.Interop;
using Microsoft.Isam.Esent.Interop.Server2003;
using Microsoft.Isam.Esent.Interop.Windows8;
using Microsoft.Isam.Esent.Interop.Windows81;
using NLog;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Reactive.Disposables;
using System.Threading;

namespace BitSharp.Esent
{
    internal class ChainStateCursor : IChainStateCursor
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        // unique per-instance session context for JetSetSessionContext
        private static int nextCursorContext;
        private readonly IntPtr cursorContext = new IntPtr(Interlocked.Increment(ref nextCursorContext));

        private readonly string jetDatabase;
        private readonly Instance jetInstance;

        public readonly Session jetSession;
        public readonly JET_DBID chainStateDbId;

        public readonly JET_TABLEID globalsTableId;
        public readonly JET_COLUMNID chainTipColumnId;
        public readonly JET_COLUMNID unspentTxCountColumnId;
        public readonly JET_COLUMNID unspentOutputCountColumnId;
        public readonly JET_COLUMNID totalTxCountColumnId;
        public readonly JET_COLUMNID totalInputCountColumnId;
        public readonly JET_COLUMNID totalOutputCountColumnId;

        public readonly JET_TABLEID flushTableId;
        public readonly JET_COLUMNID flushColumnId;

        public readonly JET_TABLEID headersTableId;
        public readonly JET_COLUMNID headerBlockHashColumnId;
        public readonly JET_COLUMNID headerBytesColumnId;

        public readonly JET_TABLEID unspentTxTableId;
        public readonly JET_COLUMNID txHashColumnId;
        public readonly JET_COLUMNID blockIndexColumnId;
        public readonly JET_COLUMNID txIndexColumnId;
        public readonly JET_COLUMNID outputStatesColumnId;

        public readonly JET_TABLEID spentTxTableId;
        public readonly JET_COLUMNID spentSpentBlockIndexColumnId;
        public readonly JET_COLUMNID spentDataColumnId;

        public readonly JET_TABLEID unmintedTxTableId;
        public readonly JET_COLUMNID unmintedBlockHashColumnId;
        public readonly JET_COLUMNID unmintedDataColumnId;

        private bool inTransaction;
        private bool readOnly;

        private bool disposed;

        public ChainStateCursor(string jetDatabase, Instance jetInstance)
        {
            this.jetDatabase = jetDatabase;
            this.jetInstance = jetInstance;

            this.OpenCursor(this.jetDatabase, this.jetInstance,
                out this.jetSession,
                out this.chainStateDbId,
                out this.globalsTableId,
                    out this.chainTipColumnId,
                    out this.unspentTxCountColumnId,
                    out this.unspentOutputCountColumnId,
                    out this.totalTxCountColumnId,
                    out this.totalInputCountColumnId,
                    out this.totalOutputCountColumnId,
                out this.flushTableId,
                    out this.flushColumnId,
                out this.headersTableId,
                    out this.headerBlockHashColumnId,
                    out headerBytesColumnId,
                out this.unspentTxTableId,
                    out this.txHashColumnId,
                    out this.blockIndexColumnId,
                    out this.txIndexColumnId,
                    out this.outputStatesColumnId,
                out spentTxTableId,
                    out spentSpentBlockIndexColumnId,
                    out spentDataColumnId,
                out unmintedTxTableId,
                    out unmintedBlockHashColumnId,
                    out unmintedDataColumnId);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposed && disposing)
            {
                Api.JetCloseDatabase(this.jetSession, this.chainStateDbId, CloseDatabaseGrbit.None);
                this.jetSession.Dispose();

                disposed = true;
            }
        }

        public bool InTransaction
        {
            get { return this.inTransaction; }
        }

        public ChainedHeader ChainTip
        {
            get
            {
                CheckTransaction();

                using (SetSessionContext())
                {
                    var chainTipBytes = Api.RetrieveColumn(this.jetSession, this.globalsTableId, this.chainTipColumnId);
                    if (chainTipBytes != null)
                        return DataEncoder.DecodeChainedHeader(chainTipBytes);
                    else
                        return null;
                }
            }
            set
            {
                CheckWriteTransaction();

                using (SetSessionContext())
                using (var jetUpdate = this.jetSession.BeginUpdate(this.globalsTableId, JET_prep.Replace))
                {
                    var chainTipBytes = value != null ? DataEncoder.EncodeChainedHeader(value) : null;
                    Api.SetColumn(this.jetSession, this.globalsTableId, this.chainTipColumnId, chainTipBytes);
                    jetUpdate.Save();
                }
            }
        }

        public int UnspentTxCount
        {
            get
            {
                CheckTransaction();
                using (SetSessionContext())
                    return Api.RetrieveColumnAsInt32(this.jetSession, this.globalsTableId, this.unspentTxCountColumnId).Value;
            }
            set
            {
                CheckWriteTransaction();

                using (SetSessionContext())
                using (var jetUpdate = this.jetSession.BeginUpdate(this.globalsTableId, JET_prep.Replace))
                {
                    Api.SetColumn(this.jetSession, this.globalsTableId, this.unspentTxCountColumnId, value);
                    jetUpdate.Save();
                }
            }
        }

        public int UnspentOutputCount
        {
            get
            {
                CheckTransaction();
                using (SetSessionContext())
                    return Api.RetrieveColumnAsInt32(this.jetSession, this.globalsTableId, this.unspentOutputCountColumnId).Value;
            }
            set
            {
                CheckWriteTransaction();

                using (SetSessionContext())
                using (var jetUpdate = this.jetSession.BeginUpdate(this.globalsTableId, JET_prep.Replace))
                {
                    Api.SetColumn(this.jetSession, this.globalsTableId, this.unspentOutputCountColumnId, value);
                    jetUpdate.Save();
                }
            }
        }

        public int TotalTxCount
        {
            get
            {
                CheckTransaction();
                using (SetSessionContext())
                    return Api.RetrieveColumnAsInt32(this.jetSession, this.globalsTableId, this.totalTxCountColumnId).Value;
            }
            set
            {
                CheckWriteTransaction();

                using (SetSessionContext())
                using (var jetUpdate = this.jetSession.BeginUpdate(this.globalsTableId, JET_prep.Replace))
                {
                    Api.SetColumn(this.jetSession, this.globalsTableId, this.totalTxCountColumnId, value);
                    jetUpdate.Save();
                }
            }
        }

        public int TotalInputCount
        {
            get
            {
                CheckTransaction();
                using (SetSessionContext())
                    return Api.RetrieveColumnAsInt32(this.jetSession, this.globalsTableId, this.totalInputCountColumnId).Value;
            }
            set
            {
                CheckWriteTransaction();

                using (SetSessionContext())
                using (var jetUpdate = this.jetSession.BeginUpdate(this.globalsTableId, JET_prep.Replace))
                {
                    Api.SetColumn(this.jetSession, this.globalsTableId, this.totalInputCountColumnId, value);
                    jetUpdate.Save();
                }
            }
        }

        public int TotalOutputCount
        {
            get
            {
                CheckTransaction();
                using (SetSessionContext())
                    return Api.RetrieveColumnAsInt32(this.jetSession, this.globalsTableId, this.totalOutputCountColumnId).Value;
            }
            set
            {
                CheckWriteTransaction();

                using (SetSessionContext())
                using (var jetUpdate = this.jetSession.BeginUpdate(this.globalsTableId, JET_prep.Replace))
                {
                    Api.SetColumn(this.jetSession, this.globalsTableId, this.totalOutputCountColumnId, value);
                    jetUpdate.Save();
                }
            }
        }

        public bool ContainsHeader(UInt256 blockHash)
        {
            CheckTransaction();

            using (SetSessionContext())
            {
                Api.JetSetCurrentIndex(this.jetSession, this.headersTableId, "IX_BlockHash");
                Api.MakeKey(this.jetSession, this.headersTableId, DbEncoder.EncodeUInt256(blockHash), MakeKeyGrbit.NewKey);
                return Api.TrySeek(this.jetSession, this.headersTableId, SeekGrbit.SeekEQ);
            }
        }

        public bool TryGetHeader(UInt256 blockHash, out ChainedHeader header)
        {
            CheckTransaction();

            using (SetSessionContext())
            {
                Api.JetSetCurrentIndex(this.jetSession, this.headersTableId, "IX_BlockHash");
                Api.MakeKey(this.jetSession, this.headersTableId, DbEncoder.EncodeUInt256(blockHash), MakeKeyGrbit.NewKey);
                if (Api.TrySeek(this.jetSession, this.headersTableId, SeekGrbit.SeekEQ))
                {
                    var headerBytes = Api.RetrieveColumn(this.jetSession, this.headersTableId, this.headerBytesColumnId);

                    header = DataEncoder.DecodeChainedHeader(headerBytes);
                    return true;
                }

                header = default(ChainedHeader);
                return false;
            }
        }

        public bool TryAddHeader(ChainedHeader header)
        {
            CheckWriteTransaction();

            try
            {
                using (SetSessionContext())
                using (var jetUpdate = this.jetSession.BeginUpdate(this.headersTableId, JET_prep.Insert))
                {
                    Api.SetColumns(this.jetSession, this.headersTableId,
                        new BytesColumnValue { Columnid = this.headerBlockHashColumnId, Value = DbEncoder.EncodeUInt256(header.Hash) },
                        new BytesColumnValue { Columnid = this.headerBytesColumnId, Value = DataEncoder.EncodeChainedHeader(header) });

                    jetUpdate.Save();
                }

                return true;
            }
            catch (EsentKeyDuplicateException)
            {
                return false;
            }
        }

        public bool TryRemoveHeader(UInt256 blockHash)
        {
            CheckWriteTransaction();

            using (SetSessionContext())
            {
                Api.JetSetCurrentIndex(this.jetSession, this.headersTableId, "IX_BlockHash");
                Api.MakeKey(this.jetSession, this.headersTableId, DbEncoder.EncodeUInt256(blockHash), MakeKeyGrbit.NewKey);
                if (Api.TrySeek(this.jetSession, this.headersTableId, SeekGrbit.SeekEQ))
                {
                    Api.JetDelete(this.jetSession, this.headersTableId);

                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

        public bool ContainsUnspentTx(UInt256 txHash)
        {
            CheckTransaction();

            using (SetSessionContext())
            {
                Api.JetSetCurrentIndex(this.jetSession, this.unspentTxTableId, "IX_TxHash");
                Api.MakeKey(this.jetSession, this.unspentTxTableId, DbEncoder.EncodeUInt256(txHash), MakeKeyGrbit.NewKey);
                return Api.TrySeek(this.jetSession, this.unspentTxTableId, SeekGrbit.SeekEQ);
            }
        }

        public bool TryGetUnspentTx(UInt256 txHash, out UnspentTx unspentTx)
        {
            CheckTransaction();

            using (SetSessionContext())
            {
                Api.JetSetCurrentIndex(this.jetSession, this.unspentTxTableId, "IX_TxHash");
                Api.MakeKey(this.jetSession, this.unspentTxTableId, DbEncoder.EncodeUInt256(txHash), MakeKeyGrbit.NewKey);
                if (Api.TrySeek(this.jetSession, this.unspentTxTableId, SeekGrbit.SeekEQ))
                {
                    var blockIndexColumn = new Int32ColumnValue { Columnid = this.blockIndexColumnId };
                    var txIndexColumn = new Int32ColumnValue { Columnid = this.txIndexColumnId };
                    var outputStatesColumn = new BytesColumnValue { Columnid = this.outputStatesColumnId };
                    Api.RetrieveColumns(this.jetSession, this.unspentTxTableId, blockIndexColumn, txIndexColumn, outputStatesColumn);

                    var blockIndex = blockIndexColumn.Value.Value;
                    var txIndex = txIndexColumn.Value.Value;
                    var outputStates = DataEncoder.DecodeOutputStates(outputStatesColumn.Value);

                    unspentTx = new UnspentTx(txHash, blockIndex, txIndex, outputStates);
                    return true;
                }

                unspentTx = default(UnspentTx);
                return false;
            }
        }

        public bool TryAddUnspentTx(UnspentTx unspentTx)
        {
            CheckWriteTransaction();

            using (SetSessionContext())
            {
                try
                {
                    using (var jetUpdate = this.jetSession.BeginUpdate(this.unspentTxTableId, JET_prep.Insert))
                    {
                        Api.SetColumns(this.jetSession, this.unspentTxTableId,
                            new BytesColumnValue { Columnid = this.txHashColumnId, Value = DbEncoder.EncodeUInt256(unspentTx.TxHash) },
                            new Int32ColumnValue { Columnid = this.blockIndexColumnId, Value = unspentTx.BlockIndex },
                            new Int32ColumnValue { Columnid = this.txIndexColumnId, Value = unspentTx.TxIndex },
                            new BytesColumnValue { Columnid = this.outputStatesColumnId, Value = DataEncoder.EncodeOutputStates(unspentTx.OutputStates) });

                        jetUpdate.Save();
                    }

                    return true;
                }
                catch (EsentKeyDuplicateException)
                {
                    return false;
                }
            }
        }

        public bool TryRemoveUnspentTx(UInt256 txHash)
        {
            CheckWriteTransaction();

            using (SetSessionContext())
            {
                Api.JetSetCurrentIndex(this.jetSession, this.unspentTxTableId, "IX_TxHash");
                Api.MakeKey(this.jetSession, this.unspentTxTableId, DbEncoder.EncodeUInt256(txHash), MakeKeyGrbit.NewKey);
                if (Api.TrySeek(this.jetSession, this.unspentTxTableId, SeekGrbit.SeekEQ))
                {
                    Api.JetDelete(this.jetSession, this.unspentTxTableId);

                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

        public bool TryUpdateUnspentTx(UnspentTx unspentTx)
        {
            CheckWriteTransaction();

            using (SetSessionContext())
            {
                Api.JetSetCurrentIndex(this.jetSession, this.unspentTxTableId, "IX_TxHash");
                Api.MakeKey(this.jetSession, this.unspentTxTableId, DbEncoder.EncodeUInt256(unspentTx.TxHash), MakeKeyGrbit.NewKey);

                if (Api.TrySeek(this.jetSession, this.unspentTxTableId, SeekGrbit.SeekEQ))
                {
                    using (var jetUpdate = this.jetSession.BeginUpdate(this.unspentTxTableId, JET_prep.Replace))
                    {
                        Api.SetColumn(this.jetSession, this.unspentTxTableId, this.outputStatesColumnId, DataEncoder.EncodeOutputStates(unspentTx.OutputStates));

                        jetUpdate.Save();
                    }

                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

        public IEnumerable<UnspentTx> ReadUnspentTransactions()
        {
            CheckTransaction();
            return ReadUnspentTransactionsInner();
        }

        private IEnumerable<UnspentTx> ReadUnspentTransactionsInner()
        {
            using (SetSessionContext())
            {
                Api.JetSetCurrentIndex(this.jetSession, this.unspentTxTableId, "IX_TxHash");

                if (Api.TryMoveFirst(this.jetSession, this.unspentTxTableId))
                {
                    do
                    {
                        var txHashColumn = new BytesColumnValue { Columnid = this.txHashColumnId };
                        var blockIndexColumn = new Int32ColumnValue { Columnid = this.blockIndexColumnId };
                        var txIndexColumn = new Int32ColumnValue { Columnid = this.txIndexColumnId };
                        var outputStatesColumn = new BytesColumnValue { Columnid = this.outputStatesColumnId };
                        Api.RetrieveColumns(this.jetSession, this.unspentTxTableId, txHashColumn, blockIndexColumn, txIndexColumn, outputStatesColumn);

                        var txHash = DbEncoder.DecodeUInt256(txHashColumn.Value);
                        var blockIndex = blockIndexColumn.Value.Value;
                        var txIndex = txIndexColumn.Value.Value;
                        var outputStates = DataEncoder.DecodeOutputStates(outputStatesColumn.Value);

                        yield return new UnspentTx(txHash, blockIndex, txIndex, outputStates);
                    }
                    while (Api.TryMoveNext(this.jetSession, this.unspentTxTableId));
                }
            }
        }

        public bool ContainsBlockSpentTxes(int blockIndex)
        {
            CheckTransaction();

            using (SetSessionContext())
            {
                Api.JetSetCurrentIndex(this.jetSession, this.spentTxTableId, "IX_SpentBlockIndex");
                Api.MakeKey(this.jetSession, this.spentTxTableId, blockIndex, MakeKeyGrbit.NewKey);
                return Api.TrySeek(this.jetSession, this.spentTxTableId, SeekGrbit.SeekEQ);
            }
        }

        public bool TryGetBlockSpentTxes(int blockIndex, out BlockSpentTxes spentTxes)
        {
            CheckTransaction();

            using (SetSessionContext())
            {
                Api.JetSetCurrentIndex(this.jetSession, this.spentTxTableId, "IX_SpentBlockIndex");

                Api.MakeKey(this.jetSession, this.spentTxTableId, blockIndex, MakeKeyGrbit.NewKey);

                if (Api.TrySeek(this.jetSession, this.spentTxTableId, SeekGrbit.SeekEQ))
                {
                    var spentTxesBytes = Api.RetrieveColumn(this.jetSession, this.spentTxTableId, this.spentDataColumnId);

                    using (var stream = new MemoryStream(spentTxesBytes))
                    using (var reader = new BinaryReader(stream))
                    {
                        spentTxes = BlockSpentTxes.CreateRange(reader.ReadList(() => DataEncoder.DecodeSpentTx(reader)));
                    }

                    return true;
                }
                else
                {
                    spentTxes = null;
                    return false;
                }
            }
        }

        public bool TryAddBlockSpentTxes(int blockIndex, BlockSpentTxes spentTxes)
        {
            CheckWriteTransaction();

            try
            {
                using (SetSessionContext())
                using (var jetUpdate = this.jetSession.BeginUpdate(this.spentTxTableId, JET_prep.Insert))
                {
                    byte[] spentTxesBytes;
                    using (var stream = new MemoryStream())
                    using (var writer = new BinaryWriter(stream))
                    {
                        writer.WriteList<SpentTx>(spentTxes, spentTx => DataEncoder.EncodeSpentTx(writer, spentTx));
                        spentTxesBytes = stream.ToArray();
                    }

                    Api.SetColumns(this.jetSession, this.spentTxTableId,
                        new Int32ColumnValue { Columnid = this.spentSpentBlockIndexColumnId, Value = blockIndex },
                        new BytesColumnValue { Columnid = this.spentDataColumnId, Value = spentTxesBytes });

                    jetUpdate.Save();
                }

                return true;
            }
            catch (EsentKeyDuplicateException)
            {
                return false;
            }
        }

        public bool TryRemoveBlockSpentTxes(int blockIndex)
        {
            CheckWriteTransaction();

            using (SetSessionContext())
            {
                Api.JetSetCurrentIndex(this.jetSession, this.spentTxTableId, "IX_SpentBlockIndex");

                Api.MakeKey(this.jetSession, this.spentTxTableId, blockIndex, MakeKeyGrbit.NewKey);

                if (Api.TrySeek(this.jetSession, this.spentTxTableId, SeekGrbit.SeekEQ))
                {
                    Api.JetDelete(this.jetSession, this.spentTxTableId);
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

        public bool ContainsBlockUnmintedTxes(UInt256 blockHash)
        {
            CheckTransaction();

            using (SetSessionContext())
            {
                Api.JetSetCurrentIndex(this.jetSession, this.unmintedTxTableId, "IX_UnmintedBlockHash");
                Api.MakeKey(this.jetSession, this.unmintedTxTableId, DbEncoder.EncodeUInt256(blockHash), MakeKeyGrbit.NewKey);
                return Api.TrySeek(this.jetSession, this.unmintedTxTableId, SeekGrbit.SeekEQ);
            }
        }

        public bool TryGetBlockUnmintedTxes(UInt256 blockHash, out IImmutableList<UnmintedTx> unmintedTxes)
        {
            CheckTransaction();

            using (SetSessionContext())
            {
                Api.JetSetCurrentIndex(this.jetSession, this.unmintedTxTableId, "IX_UnmintedBlockHash");

                Api.MakeKey(this.jetSession, this.unmintedTxTableId, DbEncoder.EncodeUInt256(blockHash), MakeKeyGrbit.NewKey);

                if (Api.TrySeek(this.jetSession, this.unmintedTxTableId, SeekGrbit.SeekEQ))
                {
                    var unmintedTxesBytes = Api.RetrieveColumn(this.jetSession, this.unmintedTxTableId, this.unmintedDataColumnId);

                    using (var stream = new MemoryStream(unmintedTxesBytes))
                    using (var reader = new BinaryReader(stream))
                    {
                        unmintedTxes = ImmutableList.CreateRange(reader.ReadList(() => DataEncoder.DecodeUnmintedTx(reader)));
                    }

                    return true;
                }
                else
                {
                    unmintedTxes = null;
                    return false;
                }
            }
        }

        public bool TryAddBlockUnmintedTxes(UInt256 blockHash, IImmutableList<UnmintedTx> unmintedTxes)
        {
            CheckWriteTransaction();

            try
            {
                using (SetSessionContext())
                using (var jetUpdate = this.jetSession.BeginUpdate(this.unmintedTxTableId, JET_prep.Insert))
                {
                    byte[] unmintedTxesBytes;
                    using (var stream = new MemoryStream())
                    using (var writer = new BinaryWriter(stream))
                    {
                        writer.WriteList(unmintedTxes, unmintedTx => DataEncoder.EncodeUnmintedTx(writer, unmintedTx));
                        unmintedTxesBytes = stream.ToArray();
                    }

                    Api.SetColumns(this.jetSession, this.unmintedTxTableId,
                        new BytesColumnValue { Columnid = this.unmintedBlockHashColumnId, Value = DbEncoder.EncodeUInt256(blockHash) },
                        new BytesColumnValue { Columnid = this.unmintedDataColumnId, Value = unmintedTxesBytes });

                    jetUpdate.Save();
                }

                return true;
            }
            catch (EsentKeyDuplicateException)
            {
                return false;
            }
        }

        public bool TryRemoveBlockUnmintedTxes(UInt256 blockHash)
        {
            CheckWriteTransaction();

            using (SetSessionContext())
            {
                Api.JetSetCurrentIndex(this.jetSession, this.unmintedTxTableId, "IX_UnmintedBlockHash");

                Api.MakeKey(this.jetSession, this.unmintedTxTableId, DbEncoder.EncodeUInt256(blockHash), MakeKeyGrbit.NewKey);

                if (Api.TrySeek(this.jetSession, this.unmintedTxTableId, SeekGrbit.SeekEQ))
                {
                    Api.JetDelete(this.jetSession, this.unmintedTxTableId);
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

        public void BeginTransaction(bool readOnly, bool pruneOnly)
        {
            if (this.inTransaction)
                throw new InvalidOperationException();

            using (SetSessionContext())
                Api.JetBeginTransaction2(this.jetSession, readOnly ? BeginTransactionGrbit.ReadOnly : BeginTransactionGrbit.None);

            this.inTransaction = true;
            this.readOnly = readOnly;
        }

        public void CommitTransaction()
        {
            if (!this.inTransaction)
                throw new InvalidOperationException();

            using (SetSessionContext())
                if (!this.readOnly)
                    Api.JetCommitTransaction(this.jetSession, CommitTransactionGrbit.LazyFlush);
                else
                    Api.JetRollback(this.jetSession, RollbackTransactionGrbit.None);

            this.inTransaction = false;
        }

        public void RollbackTransaction()
        {
            if (!this.inTransaction)
                throw new InvalidOperationException();

            using (SetSessionContext())
                Api.JetRollback(this.jetSession, RollbackTransactionGrbit.None);

            this.inTransaction = false;
        }

        public void Flush()
        {
            using (SetSessionContext())
            {
                if (EsentVersion.SupportsServer2003Features)
                {
                    Api.JetCommitTransaction(this.jetSession, Server2003Grbits.WaitAllLevel0Commit);
                }
                else
                {
                    using (var jetTx = this.jetSession.BeginTransaction())
                    {
                        Api.EscrowUpdate(this.jetSession, this.flushTableId, this.flushColumnId, 1);
                        jetTx.Commit(CommitTransactionGrbit.None);
                    }
                }
            }
        }

        public void Defragment()
        {
            using (SetSessionContext())
            {
                //int passes = int.MaxValue, seconds = int.MaxValue;
                //Api.JetDefragment(this.jetSession, this.chainStateDbId, "", ref passes, ref seconds, DefragGrbit.BatchStart);

                if (EsentVersion.SupportsWindows81Features)
                {
                    logger.Info("Begin shrinking chain state database");

                    int actualPages;
                    Windows8Api.JetResizeDatabase(this.jetSession, this.chainStateDbId, 0, out actualPages, Windows81Grbits.OnlyShrink);

                    logger.Info($"Finished shrinking chain state database: {(float)actualPages * SystemParameters.DatabasePageSize / 1.MILLION():N0} MB");
                }
            }
        }

        private void OpenCursor(string jetDatabase, Instance jetInstance,
            out Session jetSession,
            out JET_DBID chainStateDbId,
            out JET_TABLEID globalsTableId,
            out JET_COLUMNID chainTipColumnId,
            out JET_COLUMNID unspentTxCountColumnId,
            out JET_COLUMNID unspentOutputCountColumnId,
            out JET_COLUMNID totalTxCountColumnId,
            out JET_COLUMNID totalInputCountColumnId,
            out JET_COLUMNID totalOutputCountColumnId,
            out JET_TABLEID flushTableId,
            out JET_COLUMNID flushColumnId,
            out JET_TABLEID headersTableId,
            out JET_COLUMNID headerBlockHashColumnId,
            out JET_COLUMNID headerBytesColumnId,
            out JET_TABLEID unspentTxTableId,
            out JET_COLUMNID txHashColumnId,
            out JET_COLUMNID blockIndexColumnId,
            out JET_COLUMNID txIndexColumnId,
            out JET_COLUMNID outputStatesColumnId,
            out JET_TABLEID spentTxTableId,
            out JET_COLUMNID spentSpentBlockIndexColumnId,
            out JET_COLUMNID spentDataColumnId,
            out JET_TABLEID unmintedTxTableId,
            out JET_COLUMNID unmintedBlockHashColumnId,
            out JET_COLUMNID unmintedDataColumnId)
        {
            var success = false;
            jetSession = new Session(jetInstance);
            try
            {
                Api.JetOpenDatabase(jetSession, jetDatabase, "", out chainStateDbId, OpenDatabaseGrbit.None);

                Api.JetOpenTable(jetSession, chainStateDbId, "Globals", null, 0, OpenTableGrbit.None, out globalsTableId);
                chainTipColumnId = Api.GetTableColumnid(jetSession, globalsTableId, "ChainTip");
                unspentTxCountColumnId = Api.GetTableColumnid(jetSession, globalsTableId, "UnspentTxCount");
                unspentOutputCountColumnId = Api.GetTableColumnid(jetSession, globalsTableId, "UnspentOutputCount");
                totalTxCountColumnId = Api.GetTableColumnid(jetSession, globalsTableId, "TotalTxCount");
                totalInputCountColumnId = Api.GetTableColumnid(jetSession, globalsTableId, "TotalInputCount");
                totalOutputCountColumnId = Api.GetTableColumnid(jetSession, globalsTableId, "TotalOutputCount");

                if (!Api.TryMoveFirst(jetSession, globalsTableId))
                    throw new InvalidOperationException();

                Api.JetOpenTable(jetSession, chainStateDbId, "Flush", null, 0, OpenTableGrbit.None, out flushTableId);
                flushColumnId = Api.GetTableColumnid(jetSession, flushTableId, "Flush");

                if (!Api.TryMoveFirst(jetSession, flushTableId))
                    throw new InvalidOperationException();

                Api.JetOpenTable(jetSession, chainStateDbId, "Headers", null, 0, OpenTableGrbit.None, out headersTableId);
                headerBlockHashColumnId = Api.GetTableColumnid(jetSession, headersTableId, "BlockHash");
                headerBytesColumnId = Api.GetTableColumnid(jetSession, headersTableId, "HeaderBytes");

                Api.JetOpenTable(jetSession, chainStateDbId, "UnspentTx", null, 0, OpenTableGrbit.None, out unspentTxTableId);
                txHashColumnId = Api.GetTableColumnid(jetSession, unspentTxTableId, "TxHash");
                blockIndexColumnId = Api.GetTableColumnid(jetSession, unspentTxTableId, "BlockIndex");
                txIndexColumnId = Api.GetTableColumnid(jetSession, unspentTxTableId, "TxIndex");
                outputStatesColumnId = Api.GetTableColumnid(jetSession, unspentTxTableId, "OutputStates");

                Api.JetOpenTable(jetSession, chainStateDbId, "SpentTx", null, 0, OpenTableGrbit.None, out spentTxTableId);
                spentSpentBlockIndexColumnId = Api.GetTableColumnid(jetSession, spentTxTableId, "SpentBlockIndex");
                spentDataColumnId = Api.GetTableColumnid(jetSession, spentTxTableId, "SpentData");

                Api.JetOpenTable(jetSession, chainStateDbId, "UnmintedTx", null, 0, OpenTableGrbit.None, out unmintedTxTableId);
                unmintedBlockHashColumnId = Api.GetTableColumnid(jetSession, unmintedTxTableId, "BlockHash");
                unmintedDataColumnId = Api.GetTableColumnid(jetSession, unmintedTxTableId, "UnmintedData");

                success = true;
            }
            finally
            {
                if (!success)
                    jetSession.Dispose();
            }
        }

        private void CheckTransaction()
        {
            if (!this.inTransaction)
                throw new InvalidOperationException();
        }

        private void CheckWriteTransaction()
        {
            if (!this.inTransaction || this.readOnly)
                throw new InvalidOperationException();
        }

        private IDisposable SetSessionContext()
        {
            Api.JetSetSessionContext(this.jetSession, cursorContext);
            return Disposable.Create(() =>
                Api.JetResetSessionContext(this.jetSession));
        }
    }
}
