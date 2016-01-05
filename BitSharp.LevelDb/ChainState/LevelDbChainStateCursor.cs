using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using BitSharp.Core.ExtensionMethods;
using BitSharp.Core.Storage;
using LevelDB;
using NLog;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Reactive.Disposables;
using System.Threading;

namespace BitSharp.LevelDb
{
    internal class LevelDbChainStateCursor : IChainStateCursor
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        // unique per-instance session context for JetSetSessionContext
        private static int nextCursorContext;
        private readonly IntPtr cursorContext = new IntPtr(Interlocked.Increment(ref nextCursorContext));

        private readonly string jetDatabase;
        private readonly DB db;

        private Snapshot txSnapshot;
        private ReadOptions txReadOptions;
        private WriteBatch txWriteBatch;
        private SortedDictionary<Slice, Tuple<UpdateType, Slice>> txUpdates;

        private const byte UNSPENT_TX_PREFIX = 0; // place unspent txes first so they can be iterated over without skipping other data
        private const byte GLOBAL_PREFIX = 1;
        private const byte HEADER_PREFIX = 2;
        private const byte SPENT_TXES_PREFIX = 3;
        private const byte UNMINTED_TXES_PREFIX = 4;

        private bool inTransaction;
        private bool readOnly;

        private bool disposed;

        public LevelDbChainStateCursor(string jetDatabase, DB db)
        {
            this.jetDatabase = jetDatabase;
            this.db = db;
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
                txSnapshot?.Dispose();
                //txWriteBatch.Dispose();

                disposed = true;
            }
        }

        public bool InTransaction => inTransaction;

        public ChainedHeader ChainTip
        {
            get
            {
                CheckTransaction();

                Slice value;
                if (CursorTryGet(MakeGlobalKey(GlobalValue.ChainTip), out value))
                    return DataDecoder.DecodeChainedHeader(value.ToArray());
                else
                    return null;
            }
            set
            {
                CheckWriteTransaction();

                if (value != null)
                    CursorPut(MakeGlobalKey(GlobalValue.ChainTip), DataEncoder.EncodeChainedHeader(value));
                else
                    CursorDelete(MakeGlobalKey(GlobalValue.ChainTip));
            }
        }

        public int UnspentTxCount
        {
            get
            {
                CheckTransaction();

                Slice value;
                if (CursorTryGet(MakeGlobalKey(GlobalValue.UnspentTxCount), out value))
                    return value.ToInt32();
                else
                    return 0;
            }
            set
            {
                CheckWriteTransaction();

                CursorPut(MakeGlobalKey(GlobalValue.UnspentTxCount), value);
            }
        }

        public int UnspentOutputCount
        {
            get
            {
                CheckTransaction();

                Slice value;
                if (CursorTryGet(MakeGlobalKey(GlobalValue.UnspentOutputCount), out value))
                    return value.ToInt32();
                else
                    return 0;
            }
            set
            {
                CheckWriteTransaction();

                CursorPut(MakeGlobalKey(GlobalValue.UnspentOutputCount), value);
            }
        }

        public int TotalTxCount
        {
            get
            {
                CheckTransaction();

                Slice value;
                if (CursorTryGet(MakeGlobalKey(GlobalValue.TotalTxCount), out value))
                    return value.ToInt32();
                else
                    return 0;
            }
            set
            {
                CheckWriteTransaction();

                CursorPut(MakeGlobalKey(GlobalValue.TotalTxCount), value);
            }
        }

        public int TotalInputCount
        {
            get
            {
                CheckTransaction();

                Slice value;
                if (CursorTryGet(MakeGlobalKey(GlobalValue.TotalInputCount), out value))
                    return value.ToInt32();
                else
                    return 0;
            }
            set
            {
                CheckWriteTransaction();

                CursorPut(MakeGlobalKey(GlobalValue.TotalInputCount), value);
            }
        }

        public int TotalOutputCount
        {
            get
            {
                CheckTransaction();

                Slice value;
                if (CursorTryGet(MakeGlobalKey(GlobalValue.TotalOutputCount), out value))
                    return value.ToInt32();
                else
                    return 0;
            }
            set
            {
                CheckWriteTransaction();

                CursorPut(MakeGlobalKey(GlobalValue.TotalOutputCount), value);
            }
        }

        public bool ContainsHeader(UInt256 blockHash)
        {
            CheckTransaction();

            Slice value;
            return CursorTryGet(MakeHeaderKey(blockHash), out value);
        }

        public bool TryGetHeader(UInt256 blockHash, out ChainedHeader header)
        {
            CheckTransaction();

            Slice value;
            if (CursorTryGet(MakeHeaderKey(blockHash), out value))
            {
                header = DataDecoder.DecodeChainedHeader(value.ToArray());
                return true;
            }
            else
            {
                header = default(ChainedHeader);
                return false;
            }
        }

        public bool TryAddHeader(ChainedHeader header)
        {
            CheckWriteTransaction();

            if (ContainsHeader(header.Hash))
                return false;

            CursorPut(MakeHeaderKey(header.Hash), DataEncoder.EncodeChainedHeader(header));
            return true;
        }

        public bool TryRemoveHeader(UInt256 blockHash)
        {
            CheckWriteTransaction();

            if (ContainsHeader(blockHash))
            {
                CursorDelete(MakeHeaderKey(blockHash));
                return true;
            }
            else
            {
                return false;
            }
        }

        public bool ContainsUnspentTx(UInt256 txHash)
        {
            CheckTransaction();

            Slice value;
            return CursorTryGet(MakeUnspentTxKey(txHash), out value);
        }

        public bool TryGetUnspentTx(UInt256 txHash, out UnspentTx unspentTx)
        {
            CheckTransaction();

            Slice value;
            if (CursorTryGet(MakeUnspentTxKey(txHash), out value))
            {
                unspentTx = DataDecoder.DecodeUnspentTx(value.ToArray());
                return true;
            }
            else
            {
                unspentTx = default(UnspentTx);
                return false;
            }
        }

        public bool TryAddUnspentTx(UnspentTx unspentTx)
        {
            CheckWriteTransaction();

            if (ContainsUnspentTx(unspentTx.TxHash))
                return false;

            CursorPut(MakeUnspentTxKey(unspentTx.TxHash), DataEncoder.EncodeUnspentTx(unspentTx));
            return true;
        }

        public bool TryRemoveUnspentTx(UInt256 txHash)
        {
            CheckWriteTransaction();

            if (!ContainsUnspentTx(txHash))
                return false;

            CursorDelete(MakeUnspentTxKey(txHash));
            return true;
        }

        public bool TryUpdateUnspentTx(UnspentTx unspentTx)
        {
            CheckWriteTransaction();

            if (!ContainsUnspentTx(unspentTx.TxHash))
                return false;

            CursorPut(MakeUnspentTxKey(unspentTx.TxHash), DataEncoder.EncodeUnspentTx(unspentTx));
            return true;
        }

        public IEnumerable<UnspentTx> ReadUnspentTransactions()
        {
            CheckTransaction();
            return ReadUnspentTransactionsInner();
        }

        private IEnumerable<UnspentTx> ReadUnspentTransactionsInner()
        {
            CheckTransaction();

            using (var iterator = db.NewIterator(txReadOptions))
            {
                iterator.SeekToFirst();
                while (iterator.Valid())
                {
                    var key = iterator.Key().ToArray();
                    if (key[0] == UNSPENT_TX_PREFIX)
                    {
                        if (readOnly || !txUpdates.ContainsKey(key))
                        {
                            var value = iterator.Value().ToArray();
                            yield return DataDecoder.DecodeUnspentTx(value);
                        }
                    }
                    else
                        break;

                    iterator.Next();
                }
            }

            //TODO the txUpdate unspent txes will always come at the end, not in order
            if (!readOnly)
            {
                foreach (var txUpdate in txUpdates)
                {
                    var key = txUpdate.Key.ToArray();
                    if (key[0] == UNSPENT_TX_PREFIX)
                    {
                        if (txUpdate.Value.Item1 == UpdateType.Put)
                        {
                            var value = txUpdate.Value.Item2.ToArray();
                            yield return DataDecoder.DecodeUnspentTx(value);
                        }
                    }
                    else
                        break;
                }
            }
        }

        public bool ContainsBlockSpentTxes(int blockIndex)
        {
            CheckTransaction();

            Slice value;
            return CursorTryGet(MakeSpentTxesKey(blockIndex), out value);
        }

        public bool TryGetBlockSpentTxes(int blockIndex, out BlockSpentTxes spentTxes)
        {
            CheckTransaction();

            Slice value;
            if (CursorTryGet(MakeSpentTxesKey(blockIndex), out value))
            {
                spentTxes = DataDecoder.DecodeBlockSpentTxes(value.ToArray());
                return true;

            }
            else
            {
                spentTxes = default(BlockSpentTxes);
                return false;
            }
        }

        public bool TryAddBlockSpentTxes(int blockIndex, BlockSpentTxes spentTxes)
        {
            CheckWriteTransaction();

            if (ContainsBlockSpentTxes(blockIndex))
                return false;

            CursorPut(MakeSpentTxesKey(blockIndex), DataEncoder.EncodeBlockSpentTxes(spentTxes));
            return true;
        }

        public bool TryRemoveBlockSpentTxes(int blockIndex)
        {
            CheckWriteTransaction();

            if (!ContainsBlockSpentTxes(blockIndex))
                return false;

            CursorDelete(MakeSpentTxesKey(blockIndex));
            return true;
        }

        public bool ContainsBlockUnmintedTxes(UInt256 blockHash)
        {
            CheckTransaction();

            Slice value;
            return CursorTryGet(MakeUnmintedTxesKey(blockHash), out value);
        }

        public bool TryGetBlockUnmintedTxes(UInt256 blockHash, out IImmutableList<UnmintedTx> unmintedTxes)
        {
            CheckTransaction();

            Slice value;
            if (CursorTryGet(MakeUnmintedTxesKey(blockHash), out value))
            {
                unmintedTxes = DataDecoder.DecodeUnmintedTxList(value.ToArray());
                return true;
            }
            else
            {
                unmintedTxes = default(IImmutableList<UnmintedTx>);
                return false;
            }
        }

        public bool TryAddBlockUnmintedTxes(UInt256 blockHash, IImmutableList<UnmintedTx> unmintedTxes)
        {
            CheckWriteTransaction();

            if (ContainsBlockUnmintedTxes(blockHash))
                return false;

            CursorPut(MakeUnmintedTxesKey(blockHash), DataEncoder.EncodeUnmintedTxList(unmintedTxes));
            return true;
        }

        public bool TryRemoveBlockUnmintedTxes(UInt256 blockHash)
        {
            CheckWriteTransaction();

            if (!ContainsBlockUnmintedTxes(blockHash))
                return false;

            CursorDelete(MakeUnmintedTxesKey(blockHash));
            return true;
        }

        public void BeginTransaction(bool readOnly, bool pruneOnly)
        {
            if (inTransaction)
                throw new InvalidOperationException();

            txSnapshot = db.GetSnapshot();
            txReadOptions = new ReadOptions { Snapshot = txSnapshot };
            if (!readOnly)
            {
                txWriteBatch = new WriteBatch();
                txUpdates = new SortedDictionary<Slice, Tuple<UpdateType, Slice>>();
            }
            else
            {
                txWriteBatch = null;
                txUpdates = null;
            }

            inTransaction = true;
            this.readOnly = readOnly;
        }

        public void CommitTransaction()
        {
            if (!inTransaction)
                throw new InvalidOperationException();

            if (!readOnly)
                db.Write(new WriteOptions(), txWriteBatch);

            txSnapshot.Dispose();

            txSnapshot = null;
            txReadOptions = null;
            txWriteBatch = null;
            txUpdates = null;

            inTransaction = false;
        }

        public void RollbackTransaction()
        {
            if (!inTransaction)
                throw new InvalidOperationException();

            txSnapshot.Dispose();

            txSnapshot = null;
            txReadOptions = null;
            txWriteBatch = null;
            txUpdates = null;

            inTransaction = false;
        }

        public void Flush()
        {
        }

        public void Defragment()
        {
        }

        private void CheckTransaction()
        {
            if (!inTransaction)
                throw new InvalidOperationException();
        }

        private void CheckWriteTransaction()
        {
            if (!inTransaction || readOnly)
                throw new InvalidOperationException();
        }

        private byte[] MakeGlobalKey(GlobalValue globalValue)
        {
            var key = new byte[2];
            key[0] = GLOBAL_PREFIX;
            key[1] = (byte)globalValue;

            return key;
        }

        private byte[] MakeHeaderKey(UInt256 blockHash)
        {
            var key = new byte[33];
            key[0] = HEADER_PREFIX;
            blockHash.ToByteArrayBE(key, 1);

            return key;

        }

        private byte[] MakeUnspentTxKey(UInt256 txHash)
        {
            var key = new byte[33];
            key[0] = UNSPENT_TX_PREFIX;
            txHash.ToByteArrayBE(key, 1);

            return key;
        }

        private byte[] MakeSpentTxesKey(int blockHeight)
        {
            var key = new byte[5];
            key[0] = SPENT_TXES_PREFIX;
            Buffer.BlockCopy(Bits.GetBytes(blockHeight), 0, key, 1, 4);

            return key;

        }

        private byte[] MakeUnmintedTxesKey(UInt256 blockHash)
        {
            var key = new byte[33];
            key[0] = UNMINTED_TXES_PREFIX;
            blockHash.ToByteArrayBE(key, 1);

            return key;
        }

        private void CursorPut(Slice key, Slice value)
        {
            CheckWriteTransaction();

            txWriteBatch.Put(key, value);
            txUpdates[key] = Tuple.Create(UpdateType.Put, value);
        }

        private void CursorDelete(Slice key)
        {
            CheckWriteTransaction();

            txWriteBatch.Delete(key);
            txUpdates[key] = Tuple.Create(UpdateType.Delete, default(Slice));
        }

        private bool CursorTryGet(Slice key, out Slice value)
        {
            CheckTransaction();

            Tuple<UpdateType, Slice> txUpdateValue;
            if (!readOnly && txUpdates.TryGetValue(key, out txUpdateValue))
            {
                if (txUpdateValue.Item1 == UpdateType.Put)
                {
                    value = txUpdateValue.Item2;
                    return true;
                }
                else
                {
                    value = default(Slice);
                    return false;
                }
            }
            else
            {
                return db.TryGet(txReadOptions, key, out value);
            }
        }
    }

    public enum GlobalValue : byte
    {
        ChainTip,
        UnspentTxCount,
        UnspentOutputCount,
        TotalTxCount,
        TotalInputCount,
        TotalOutputCount,
    }

    public enum UpdateType
    {
        Put,
        Delete
    }
}
