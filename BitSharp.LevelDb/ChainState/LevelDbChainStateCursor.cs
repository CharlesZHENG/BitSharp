using BitSharp.Common;
using BitSharp.Core;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage;
using LevelDB;
using NLog;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BitSharp.LevelDb
{
    internal class LevelDbChainStateCursor : IChainStateCursor, IDeferredChainStateCursor
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        // unique per-instance session context for JetSetSessionContext
        private static int nextCursorContext;
        private readonly IntPtr cursorContext = new IntPtr(Interlocked.Increment(ref nextCursorContext));

        private readonly DB db;
        private readonly bool isDeferred;

        private Snapshot txSnapshot;
        private ReadOptions txReadOptions;
        private WriteBatch txWriteBatch;

        private WorkQueueDictionary<UInt256, UnspentTx> unspentTxes;
        private WorkQueueDictionary<TxOutputKey, TxOutput> unspentTxOutputs;
        private WorkQueueDictionary<GlobalValue, Slice> globals;
        private WorkQueueDictionary<UInt256, ChainedHeader> headers;
        private WorkQueueDictionary<int, BlockSpentTxes> spentTxes;
        private WorkQueueDictionary<UInt256, IImmutableList<UnmintedTx>> unmintedTxes;

        private ActionBlock<WorkQueueDictionary<UInt256, UnspentTx>.WorkItem> unspentTxesApplier;
        private ActionBlock<WorkQueueDictionary<TxOutputKey, TxOutput>.WorkItem> unspentTxOutputsApplier;
        private ActionBlock<WorkQueueDictionary<GlobalValue, Slice>.WorkItem> globalsApplier;
        private ActionBlock<WorkQueueDictionary<UInt256, ChainedHeader>.WorkItem> headersApplier;
        private ActionBlock<WorkQueueDictionary<int, BlockSpentTxes>.WorkItem> spentTxesApplier;
        private ActionBlock<WorkQueueDictionary<UInt256, IImmutableList<UnmintedTx>>.WorkItem> unmintedTxesApplier;

        private const byte UNSPENT_TX_PREFIX = 0; // place unspent txes first so they can be iterated over without skipping other data
        private const byte UNSPENT_TX_OUTPUT_PREFIX = 1;
        private const byte GLOBAL_PREFIX = 2;
        private const byte HEADER_PREFIX = 3;
        private const byte SPENT_TXES_PREFIX = 4;
        private const byte UNMINTED_TXES_PREFIX = 5;

        private bool inTransaction;
        private bool readOnly;

        private bool disposed;

        public LevelDbChainStateCursor(DB db, bool isDeferred)
        {
            this.db = db;
            this.isDeferred = isDeferred;
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
                txWriteBatch?.Dispose();

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
                if (CursorTryGet(GlobalValue.ChainTip, out value, globals, MakeGlobalKey, x => x))
                    return DataDecoder.DecodeChainedHeader(value.ToArray());
                else
                    return null;
            }
            set
            {
                CheckWriteTransaction();

                if (value != null)
                    globals[GlobalValue.ChainTip] = DataEncoder.EncodeChainedHeader(value);
                else
                    globals.TryRemove(GlobalValue.ChainTip);
            }
        }

        public int UnspentTxCount
        {
            get
            {
                CheckTransaction();

                Slice value;
                if (CursorTryGet(GlobalValue.UnspentTxCount, out value, globals, MakeGlobalKey, x => x))
                    return value.ToInt32();
                else
                    return 0;
            }
            set
            {
                CheckWriteTransaction();

                globals[GlobalValue.UnspentTxCount] = value;
            }
        }

        public int UnspentOutputCount
        {
            get
            {
                CheckTransaction();

                Slice value;
                if (CursorTryGet(GlobalValue.UnspentOutputCount, out value, globals, MakeGlobalKey, x => x))
                    return value.ToInt32();
                else
                    return 0;
            }
            set
            {
                CheckWriteTransaction();

                globals[GlobalValue.UnspentOutputCount] = value;
            }
        }

        public int TotalTxCount
        {
            get
            {
                CheckTransaction();

                Slice value;
                if (CursorTryGet(GlobalValue.TotalTxCount, out value, globals, MakeGlobalKey, x => x))
                    return value.ToInt32();
                else
                    return 0;
            }
            set
            {
                CheckWriteTransaction();

                globals[GlobalValue.TotalTxCount] = value;
            }
        }

        public int TotalInputCount
        {
            get
            {
                CheckTransaction();

                Slice value;
                if (CursorTryGet(GlobalValue.TotalInputCount, out value, globals, MakeGlobalKey, x => x))
                    return value.ToInt32();
                else
                    return 0;
            }
            set
            {
                CheckWriteTransaction();

                globals[GlobalValue.TotalInputCount] = value;
            }
        }

        public int TotalOutputCount
        {
            get
            {
                CheckTransaction();

                Slice value;
                if (CursorTryGet(GlobalValue.TotalOutputCount, out value, globals, MakeGlobalKey, x => x))
                    return value.ToInt32();
                else
                    return 0;
            }
            set
            {
                CheckWriteTransaction();

                globals[GlobalValue.TotalOutputCount] = value;
            }
        }

        public int CursorCount
        {
            get
            {
                return int.MaxValue;
            }
        }

        public IDataflowBlock[] DataFlowBlocks =>
            new IDataflowBlock[]
            {
                unspentTxes.WorkQueue,
                unspentTxOutputs.WorkQueue,
                globals.WorkQueue,
                headers.WorkQueue,
                spentTxes.WorkQueue,
                unmintedTxes.WorkQueue,
                unspentTxesApplier,
                unspentTxOutputsApplier,
                globalsApplier,
                headersApplier,
                spentTxesApplier,
                unmintedTxesApplier,
            };

        public bool ContainsHeader(UInt256 blockHash)
        {
            CheckTransaction();

            return CursorContains(blockHash, headers, MakeHeaderKey);
        }

        public bool TryGetHeader(UInt256 blockHash, out ChainedHeader header)
        {
            CheckTransaction();

            return CursorTryGet(blockHash, out header, headers, MakeHeaderKey, x => DataDecoder.DecodeChainedHeader(x));
        }

        public bool TryAddHeader(ChainedHeader header)
        {
            CheckWriteTransaction();

            return headers.TryAdd(header.Hash, header);
        }

        public bool TryRemoveHeader(UInt256 blockHash)
        {
            CheckWriteTransaction();

            return headers.TryRemove(blockHash);
        }

        public bool ContainsUnspentTx(UInt256 txHash)
        {
            CheckTransaction();

            return CursorContains(txHash, unspentTxes, MakeUnspentTxKey);
        }

        public bool TryGetUnspentTx(UInt256 txHash, out UnspentTx unspentTx)
        {
            CheckTransaction();

            return CursorTryGet(txHash, out unspentTx, unspentTxes, MakeUnspentTxKey, x => DataDecoder.DecodeUnspentTx(x));
        }

        public bool TryAddUnspentTx(UnspentTx unspentTx)
        {
            CheckWriteTransaction();

            return unspentTxes.TryAdd(unspentTx.TxHash, unspentTx);
        }

        public bool TryRemoveUnspentTx(UInt256 txHash)
        {
            CheckWriteTransaction();

            return unspentTxes.TryRemove(txHash);
        }

        public void RemoveUnspentTx(UInt256 txHash)
        {
            CheckWriteTransaction();

            unspentTxes.Remove(txHash);
        }

        public bool TryUpdateUnspentTx(UnspentTx unspentTx)
        {
            CheckWriteTransaction();

            return unspentTxes.TryUpdate(unspentTx.TxHash, unspentTx);
        }

        public IEnumerable<UnspentTx> ReadUnspentTransactions()
        {
            CheckTransaction();
            return ReadUnspentTransactionsInner();
        }

        private IEnumerable<UnspentTx> ReadUnspentTransactionsInner()
        {
            CheckTransaction();

            if (!readOnly)
                throw new InvalidOperationException();

            using (var iterator = db.NewIterator(txReadOptions))
            {
                iterator.SeekToFirst();
                while (iterator.Valid())
                {
                    var key = iterator.Key().ToArray();
                    if (key[0] == UNSPENT_TX_PREFIX)
                    {
                        var value = iterator.Value().ToArray();
                        yield return DataDecoder.DecodeUnspentTx(value);
                    }
                    else
                        break;

                    iterator.Next();
                }
            }
        }

        public bool ContainsUnspentTxOutput(TxOutputKey txOutputKey)
        {
            CheckTransaction();

            return CursorContains(txOutputKey, unspentTxOutputs, MakeUnspentTxOutputKey);
        }

        public bool TryGetUnspentTxOutput(TxOutputKey txOutputKey, out TxOutput txOutput)
        {
            CheckTransaction();

            return CursorTryGet(txOutputKey, out txOutput, unspentTxOutputs, MakeUnspentTxOutputKey, x => DataDecoder.DecodeTxOutput(x));
        }

        public bool TryAddUnspentTxOutput(TxOutputKey txOutputKey, TxOutput txOutput)
        {
            CheckWriteTransaction();

            return unspentTxOutputs.TryAdd(txOutputKey, txOutput);
        }

        public bool TryRemoveUnspentTxOutput(TxOutputKey txOutputKey)
        {
            CheckWriteTransaction();

            return unspentTxOutputs.TryRemove(txOutputKey);
        }

        public void RemoveUnspentTxOutput(TxOutputKey txOutputKey)
        {
            CheckWriteTransaction();

            unspentTxOutputs.Remove(txOutputKey);
        }

        public bool ContainsBlockSpentTxes(int blockIndex)
        {
            CheckTransaction();

            return CursorContains(blockIndex, spentTxes, MakeSpentTxesKey);
        }

        public bool TryGetBlockSpentTxes(int blockIndex, out BlockSpentTxes spentTxes)
        {
            CheckTransaction();

            return CursorTryGet(blockIndex, out spentTxes, this.spentTxes, MakeSpentTxesKey, x => DataDecoder.DecodeBlockSpentTxes(x));
        }

        public bool TryAddBlockSpentTxes(int blockIndex, BlockSpentTxes spentTxes)
        {
            CheckWriteTransaction();

            return this.spentTxes.TryAdd(blockIndex, spentTxes);
        }

        public bool TryRemoveBlockSpentTxes(int blockIndex)
        {
            CheckWriteTransaction();

            return spentTxes.TryRemove(blockIndex);
        }

        public bool ContainsBlockUnmintedTxes(UInt256 blockHash)
        {
            CheckTransaction();

            return CursorContains(blockHash, unmintedTxes, MakeUnmintedTxesKey);
        }

        public bool TryGetBlockUnmintedTxes(UInt256 blockHash, out IImmutableList<UnmintedTx> unmintedTxes)
        {
            CheckTransaction();

            return CursorTryGet(blockHash, out unmintedTxes, this.unmintedTxes, MakeUnmintedTxesKey, x => DataDecoder.DecodeUnmintedTxList(x));
        }

        public bool TryAddBlockUnmintedTxes(UInt256 blockHash, IImmutableList<UnmintedTx> unmintedTxes)
        {
            CheckWriteTransaction();

            return this.unmintedTxes.TryAdd(blockHash, unmintedTxes);
        }

        public bool TryRemoveBlockUnmintedTxes(UInt256 blockHash)
        {
            CheckWriteTransaction();

            return unmintedTxes.TryRemove(blockHash);
        }

        public void BeginTransaction(bool readOnly, bool pruneOnly)
        {
            if (inTransaction)
                throw new InvalidOperationException();

            txSnapshot = db.GetSnapshot();
            txReadOptions = new ReadOptions { Snapshot = txSnapshot };

            if (!readOnly)
                txWriteBatch = new WriteBatch();
            else
                txWriteBatch = null;

            if (!readOnly || isDeferred)
            {
                InitWorkQueueDictionaries();
            }
            else
            {
                unspentTxes = null;
                unspentTxOutputs = null;
                globals = null;
                headers = null;
                spentTxes = null;
                unmintedTxes = null;
                unspentTxOutputsApplier = null;
                unspentTxesApplier = null;
                globalsApplier = null;
                headersApplier = null;
                spentTxesApplier = null;
                unmintedTxesApplier = null;
            }


            inTransaction = true;
            this.readOnly = readOnly;
        }

        public void CommitTransaction()
        {
            CommitTransactionAsync().Wait();
        }

        public async Task CommitTransactionAsync()
        {
            if (!inTransaction)
                throw new InvalidOperationException();

            if (!readOnly)
            {
                unspentTxes.WorkQueue.Complete();
                unspentTxOutputs.WorkQueue.Complete();
                globals.WorkQueue.Complete();
                headers.WorkQueue.Complete();
                spentTxes.WorkQueue.Complete();
                unmintedTxes.WorkQueue.Complete();

                await unspentTxesApplier.Completion;
                await unspentTxOutputsApplier.Completion;
                await globalsApplier.Completion;
                await headersApplier.Completion;
                await spentTxesApplier.Completion;
                await unmintedTxesApplier.Completion;

                db.Write(new WriteOptions(), txWriteBatch);
                txWriteBatch.Dispose();
            }

            txSnapshot.Dispose();

            txSnapshot = null;
            txReadOptions = null;
            txWriteBatch = null;
            unspentTxes = null;
            unspentTxOutputs = null;
            globals = null;
            headers = null;
            spentTxes = null;
            unmintedTxes = null;
            unspentTxesApplier = null;
            unspentTxOutputsApplier = null;
            globalsApplier = null;
            headersApplier = null;
            spentTxesApplier = null;
            unmintedTxesApplier = null;

            inTransaction = false;
        }

        public void RollbackTransaction()
        {
            if (!inTransaction)
                throw new InvalidOperationException();

            if (!readOnly || isDeferred)
            {
                unspentTxes.WorkQueue.Complete();
                unspentTxOutputs.WorkQueue.Complete();
                globals.WorkQueue.Complete();
                headers.WorkQueue.Complete();
                spentTxes.WorkQueue.Complete();
                unmintedTxes.WorkQueue.Complete();
                unspentTxesApplier.Complete();
                unspentTxOutputsApplier.Complete();
                globalsApplier.Complete();
                headersApplier.Complete();
                spentTxesApplier.Complete();
                unmintedTxesApplier.Complete();
            }

            txSnapshot.Dispose();
            txWriteBatch?.Dispose();

            txSnapshot = null;
            txReadOptions = null;
            txWriteBatch = null;
            unspentTxes = null;
            unspentTxOutputs = null;
            globals = null;
            headers = null;
            spentTxes = null;
            unmintedTxes = null;
            unspentTxesApplier = null;
            unspentTxOutputsApplier = null;
            globalsApplier = null;
            headersApplier = null;
            spentTxesApplier = null;
            unmintedTxesApplier = null;

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

        private byte[] MakeUnspentTxOutputKey(TxOutputKey txOutputKey)
        {
            var key = new byte[37];
            key[0] = UNSPENT_TX_OUTPUT_PREFIX;
            txOutputKey.TxHash.ToByteArrayBE(key, 1);
            Buffer.BlockCopy(Bits.GetBytes(txOutputKey.TxOutputIndex), 0, key, 33, 4);

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

        private bool CursorContains<TKey, TValue>(TKey key, WorkQueueDictionary<TKey, TValue> dict, Func<TKey, byte[]> encodeKey)
        {
            CheckTransaction();

            if (dict != null)
                return dict.ContainsKey(key);
            else
            {
                var keySlice = encodeKey(key);
                Slice valueSlice;
                return db.TryGet(txReadOptions, keySlice, out valueSlice);
            }
        }

        private bool CursorTryGet<TKey, TValue>(TKey key, out TValue value, WorkQueueDictionary<TKey, TValue> dict, Func<TKey, byte[]> encodeKey, Func<byte[], TValue> decodeValue)
        {
            CheckTransaction();

            if (dict != null)
                return dict.TryGetValue(key, out value);
            else
            {
                var keySlice = encodeKey(key);
                Slice valueSlice;
                if (db.TryGet(txReadOptions, keySlice, out valueSlice))
                {
                    value = decodeValue(valueSlice.ToArray());
                    return true;
                }
                else
                {
                    value = default(TValue);
                    return false;
                }
            }
        }

        public void WarmUnspentTx(UInt256 txHash)
        {
            unspentTxes.WarmupValue(txHash);
        }

        public void WarmUnspentTxOutput(TxOutputKey txOutputKey)
        {
            unspentTxOutputs.WarmupValue(txOutputKey);
        }

        public async Task ApplyChangesAsync()
        {
            unspentTxes.WorkQueue.Complete();
            unspentTxOutputs.WorkQueue.Complete();
            globals.WorkQueue.Complete();
            headers.WorkQueue.Complete();
            spentTxes.WorkQueue.Complete();
            unmintedTxes.WorkQueue.Complete();

            await unspentTxesApplier.Completion;
            await unspentTxOutputsApplier.Completion;
            await globalsApplier.Completion;
            await headersApplier.Completion;
            await spentTxesApplier.Completion;
            await unmintedTxesApplier.Completion;
        }

        private void InitWorkQueueDictionaries()
        {
            unspentTxes = CreateWorkQueueDictionary<UInt256, UnspentTx>(MakeUnspentTxKey, x => DataDecoder.DecodeUnspentTx(x));
            unspentTxesApplier = CreateApplier<UInt256, UnspentTx>(MakeUnspentTxKey, x => DataEncoder.EncodeUnspentTx(x));
            unspentTxes.WorkQueue.LinkTo(unspentTxesApplier, new DataflowLinkOptions { PropagateCompletion = true });

            unspentTxOutputs = CreateWorkQueueDictionary<TxOutputKey, TxOutput>(MakeUnspentTxOutputKey, x => DataDecoder.DecodeTxOutput(x));
            unspentTxOutputsApplier = CreateApplier<TxOutputKey, TxOutput>(MakeUnspentTxOutputKey, x => DataEncoder.EncodeTxOutput(x));
            unspentTxOutputs.WorkQueue.LinkTo(unspentTxOutputsApplier, new DataflowLinkOptions { PropagateCompletion = true });

            globals = CreateWorkQueueDictionary<GlobalValue, Slice>(MakeGlobalKey, x => x);
            globalsApplier = CreateApplier<GlobalValue, Slice>(MakeGlobalKey, x => x.ToArray());
            globals.WorkQueue.LinkTo(globalsApplier, new DataflowLinkOptions { PropagateCompletion = true });

            headers = CreateWorkQueueDictionary<UInt256, ChainedHeader>(MakeHeaderKey, x => DataDecoder.DecodeChainedHeader(x));
            headersApplier = CreateApplier<UInt256, ChainedHeader>(MakeHeaderKey, x => DataEncoder.EncodeChainedHeader(x));
            headers.WorkQueue.LinkTo(headersApplier, new DataflowLinkOptions { PropagateCompletion = true });

            spentTxes = CreateWorkQueueDictionary<int, BlockSpentTxes>(MakeSpentTxesKey, x => DataDecoder.DecodeBlockSpentTxes(x));
            spentTxesApplier = CreateApplier<int, BlockSpentTxes>(MakeSpentTxesKey, x => DataEncoder.EncodeBlockSpentTxes(x));
            spentTxes.WorkQueue.LinkTo(spentTxesApplier, new DataflowLinkOptions { PropagateCompletion = true });

            unmintedTxes = CreateWorkQueueDictionary<UInt256, IImmutableList<UnmintedTx>>(MakeUnmintedTxesKey, x => DataDecoder.DecodeUnmintedTxList(x));
            unmintedTxesApplier = CreateApplier<UInt256, IImmutableList<UnmintedTx>>(MakeUnmintedTxesKey, x => DataEncoder.EncodeUnmintedTxList(x));
            unmintedTxes.WorkQueue.LinkTo(unmintedTxesApplier, new DataflowLinkOptions { PropagateCompletion = true });
        }

        private WorkQueueDictionary<TKey, TValue> CreateWorkQueueDictionary<TKey, TValue>(Func<TKey, byte[]> encodeKey, Func<byte[], TValue> decodeValue)
        {
            return new WorkQueueDictionary<TKey, TValue>(
                 key =>
                 {
                     var keySlice = (Slice)encodeKey(key);

                     Slice value;
                     if (db.TryGet(txReadOptions, keySlice, out value))
                         return Tuple.Create(true, decodeValue(value.ToArray()));
                     else
                         return Tuple.Create(false, default(TValue));
                 });
        }

        private ActionBlock<WorkQueueDictionary<TKey, TValue>.WorkItem> CreateApplier<TKey, TValue>(Func<TKey, byte[]> encodeKey, Func<TValue, byte[]> encodeValue)
        {
            return new ActionBlock<WorkQueueDictionary<TKey, TValue>.WorkItem>(
                workItem =>
                {
                    workItem.Consume(
                        (operation, key, value) =>
                        {
                            switch (operation)
                            {
                                case WorkQueueOperation.Nothing:
                                    break;

                                case WorkQueueOperation.Add:
                                case WorkQueueOperation.Update:
                                    {
                                        var keySlice = encodeKey(key);
                                        var valueSlice = encodeValue(value);
                                        lock (txWriteBatch)
                                            txWriteBatch.Put(keySlice, valueSlice);
                                    }
                                    break;

                                case WorkQueueOperation.Remove:
                                    {
                                        var keySlice = encodeKey(key);
                                        lock (txWriteBatch)
                                            txWriteBatch.Delete(keySlice);
                                    }
                                    break;

                                default:
                                    throw new InvalidOperationException();
                            }
                        });
                });
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
}
