using BitSharp.Common;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage;
using NLog;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BitSharp.Core.Builders
{
    public class DeferredChainStateCursor : IDeferredChainStateCursor
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly IChainState chainState;
        private readonly IStorageManager storageManager;

        private DeferredDictionary<UInt256, ChainedHeader> headers;
        private WorkQueueDictionary<UInt256, UnspentTx> unspentTxes;
        private DeferredDictionary<int, BlockSpentTxes> blockSpentTxes;
        private DeferredDictionary<UInt256, IImmutableList<UnmintedTx>> blockUnmintedTxes;

        private bool inTransaction;
        private DisposeHandle<IChainStateCursor> parentHandle;
        private IChainStateCursor parentCursor;
        private ActionBlock<WorkQueueDictionary<UInt256, UnspentTx>.WorkItem> utxoApplier;
        private bool changesApplied;

        public DeferredChainStateCursor(IChainState chainState, IStorageManager storageManager)
        {
            this.chainState = chainState;
            this.storageManager = storageManager;

            UnspentOutputCount = chainState.UnspentOutputCount;
            UnspentTxCount = chainState.UnspentTxCount;
            TotalTxCount = chainState.TotalTxCount;
            TotalInputCount = chainState.TotalInputCount;
            TotalOutputCount = chainState.TotalOutputCount;

            headers = new DeferredDictionary<UInt256, ChainedHeader>(
                blockHash =>
                {
                    ChainedHeader header;
                    return Tuple.Create(chainState.TryGetHeader(blockHash, out header), header);
                });

            unspentTxes = new WorkQueueDictionary<UInt256, UnspentTx>(
                txHash =>
                {
                    UnspentTx unspentTx;
                    return Tuple.Create(chainState.TryGetUnspentTx(txHash, out unspentTx), unspentTx);
                });

            blockSpentTxes = new DeferredDictionary<int, BlockSpentTxes>(
                blockHeight =>
                {
                    BlockSpentTxes spentTxes;
                    return Tuple.Create(chainState.TryGetBlockSpentTxes(blockHeight, out spentTxes), spentTxes);
                });

            blockUnmintedTxes = new DeferredDictionary<UInt256, IImmutableList<UnmintedTx>>(
                blockHash =>
                {
                    IImmutableList<UnmintedTx> unmintedTxes;
                    return Tuple.Create(chainState.TryGetBlockUnmintedTxes(blockHash, out unmintedTxes), unmintedTxes);
                });
        }

        public void Dispose()
        {
            if (inTransaction)
                RollbackTransaction();
        }

        public int CursorCount { get { return chainState.CursorCount; } }

        public bool InTransaction
        {
            get { return inTransaction; }
        }

        public void BeginTransaction(bool readOnly, bool pruneOnly)
        {
            if (inTransaction)
                throw new InvalidOperationException();

            parentHandle = storageManager.OpenChainStateCursor();
            parentCursor = parentHandle.Item;
            changesApplied = false;

            try
            {
                parentCursor.BeginTransaction();

                this.ChainTip = parentCursor.ChainTip;
                this.UnspentTxCount = UnspentTxCount;
                this.UnspentOutputCount = UnspentOutputCount;
                this.TotalTxCount = TotalTxCount;
                this.TotalInputCount = TotalInputCount;
                this.TotalOutputCount = TotalOutputCount;

                utxoApplier = new ActionBlock<WorkQueueDictionary<UInt256, UnspentTx>.WorkItem>(
                    workItem =>
                    {
                        workItem.Consume(
                            (operation, unspentTxHash, unspentTx) =>
                            {
                                switch (operation)
                                {
                                    case WorkQueueOperation.Nothing:
                                        break;

                                    case WorkQueueOperation.Add:
                                        if (!parentCursor.TryAddUnspentTx(unspentTx))
                                            throw new InvalidOperationException();
                                        break;

                                    case WorkQueueOperation.Update:
                                        if (!parentCursor.TryUpdateUnspentTx(unspentTx))
                                            throw new InvalidOperationException();
                                        break;

                                    case WorkQueueOperation.Remove:
                                        if (!parentCursor.TryRemoveUnspentTx(unspentTxHash))
                                            throw new InvalidOperationException();
                                        break;

                                    default:
                                        throw new InvalidOperationException();
                                }
                            });
                    });

                unspentTxes.WorkQueue.LinkTo(utxoApplier, new DataflowLinkOptions { PropagateCompletion = true });

                inTransaction = true;
            }
            finally
            {
                if (!inTransaction)
                {
                    parentHandle.Dispose();

                    parentHandle = null;
                    parentCursor = null;
                    utxoApplier = null;
                }
            }
        }

        public void CommitTransaction()
        {
            if (!inTransaction)
                throw new InvalidOperationException();

            parentCursor.CommitTransaction();
            parentHandle.Dispose();

            parentHandle = null;
            parentCursor = null;
            utxoApplier = null;

            inTransaction = false;
        }

        public void RollbackTransaction()
        {
            if (!inTransaction)
                throw new InvalidOperationException();

            parentCursor.RollbackTransaction();
            parentHandle.Dispose();

            parentHandle = null;
            parentCursor = null;
            utxoApplier = null;

            inTransaction = false;
        }

        public IEnumerable<ChainedHeader> ReadChain()
        {
            throw new NotSupportedException();
        }

        public ChainedHeader ChainTip { get; set; }

        public int UnspentTxCount { get; set; }

        public int UnspentOutputCount { get; set; }

        public int TotalTxCount { get; set; }

        public int TotalInputCount { get; set; }

        public int TotalOutputCount { get; set; }

        public bool ContainsHeader(UInt256 blockHash)
        {
            return headers.ContainsKey(blockHash);
        }

        public bool TryGetHeader(UInt256 blockHash, out ChainedHeader header)
        {
            return headers.TryGetValue(blockHash, out header);
        }

        public bool TryAddHeader(ChainedHeader header)
        {
            return headers.TryAdd(header.Hash, header);
        }

        public bool TryRemoveHeader(UInt256 blockHash)
        {
            return headers.TryRemove(blockHash);
        }

        public bool ContainsUnspentTx(UInt256 txHash)
        {
            return unspentTxes.ContainsKey(txHash);
        }

        public bool TryGetUnspentTx(UInt256 txHash, out UnspentTx unspentTx)
        {
            return unspentTxes.TryGetValue(txHash, out unspentTx);
        }

        public bool TryAddUnspentTx(UnspentTx unspentTx)
        {
            return unspentTxes.TryAdd(unspentTx.TxHash, unspentTx);
        }

        public bool TryRemoveUnspentTx(UInt256 txHash)
        {
            return unspentTxes.TryRemove(txHash);
        }

        public bool TryUpdateUnspentTx(UnspentTx unspentTx)
        {
            return unspentTxes.TryUpdate(unspentTx.TxHash, unspentTx);
        }

        public IEnumerable<UnspentTx> ReadUnspentTransactions()
        {
            throw new NotSupportedException();
        }

        public bool ContainsBlockSpentTxes(int blockIndex)
        {
            return blockSpentTxes.ContainsKey(blockIndex);
        }

        public bool TryGetBlockSpentTxes(int blockIndex, out BlockSpentTxes spentTxes)
        {
            return blockSpentTxes.TryGetValue(blockIndex, out spentTxes);
        }

        public bool TryAddBlockSpentTxes(int blockIndex, BlockSpentTxes spentTxes)
        {
            return blockSpentTxes.TryAdd(blockIndex, spentTxes);
        }

        public bool TryRemoveBlockSpentTxes(int blockIndex)
        {
            return blockSpentTxes.TryRemove(blockIndex);
        }

        public bool ContainsBlockUnmintedTxes(UInt256 blockHash)
        {
            return blockUnmintedTxes.ContainsKey(blockHash);
        }

        public bool TryGetBlockUnmintedTxes(UInt256 blockHash, out IImmutableList<UnmintedTx> unmintedTxes)
        {
            return blockUnmintedTxes.TryGetValue(blockHash, out unmintedTxes);
        }

        public bool TryAddBlockUnmintedTxes(UInt256 blockHash, IImmutableList<UnmintedTx> unmintedTxes)
        {
            return blockUnmintedTxes.TryAdd(blockHash, unmintedTxes);
        }

        public bool TryRemoveBlockUnmintedTxes(UInt256 blockHash)
        {
            return blockUnmintedTxes.TryRemove(blockHash);
        }

        public void Flush()
        {
            throw new NotSupportedException();
        }

        public void Defragment()
        {
            throw new NotSupportedException();
        }

        public void WarmUnspentTx(UInt256 txHash)
        {
            unspentTxes.WarmupValue(txHash, () =>
            {
                UnspentTx unspentTx;
                chainState.TryGetUnspentTx(txHash, out unspentTx);
                return unspentTx;
            });
        }

        public async Task ApplyChangesAsync()
        {
            if (!inTransaction || changesApplied)
                throw new InvalidOperationException();

            unspentTxes.WorkQueue.Complete();
            await utxoApplier.Completion;

            parentCursor.ChainTip = ChainTip;
            parentCursor.UnspentOutputCount = UnspentOutputCount;
            parentCursor.UnspentTxCount = UnspentTxCount;
            parentCursor.TotalTxCount = TotalTxCount;
            parentCursor.TotalInputCount = TotalInputCount;
            parentCursor.TotalOutputCount = TotalOutputCount;

            if (headers.Updated.Count > 0)
                throw new InvalidOperationException();
            foreach (var chainedHeader in headers.Added)
                if (!parentCursor.TryAddHeader(chainedHeader.Value))
                    throw new InvalidOperationException();
            foreach (var blockHash in headers.Deleted)
                if (!parentCursor.TryRemoveHeader(blockHash))
                    throw new InvalidOperationException();

            if (blockSpentTxes.Updated.Count > 0)
                throw new InvalidOperationException();
            foreach (var spentTxes in blockSpentTxes.Added)
                if (!parentCursor.TryAddBlockSpentTxes(spentTxes.Key, spentTxes.Value))
                    throw new InvalidOperationException();
            foreach (var blockHeight in blockSpentTxes.Deleted)
                if (!parentCursor.TryRemoveBlockSpentTxes(blockHeight))
                    throw new InvalidOperationException();

            if (blockUnmintedTxes.Updated.Count > 0)
                throw new InvalidOperationException();
            foreach (var unmintedTxes in blockUnmintedTxes.Added)
                if (!parentCursor.TryAddBlockUnmintedTxes(unmintedTxes.Key, unmintedTxes.Value))
                    throw new InvalidOperationException();
            foreach (var blockHeight in blockUnmintedTxes.Deleted)
                if (!parentCursor.TryRemoveBlockUnmintedTxes(blockHeight))
                    throw new InvalidOperationException();

            changesApplied = true;
        }
    }
}
