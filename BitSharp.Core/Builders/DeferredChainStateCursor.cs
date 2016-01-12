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

        private ChainedHeader chainTip;
        private int unspentTxCount;
        private int unspentOutputCount;
        private int totalTxCount;
        private int totalInputCount;
        private int totalOutputCount;

        private DeferredDictionary<UInt256, ChainedHeader> headers;
        private WorkQueueDictionary<UInt256, UnspentTx> unspentTxes;
        private DeferredDictionary<int, BlockSpentTxes> blockSpentTxes;
        private DeferredDictionary<UInt256, IImmutableList<UnmintedTx>> blockUnmintedTxes;

        private bool wasInTransaction;
        private bool inTransaction;
        private bool readOnly;

        private DisposeHandle<IChainStateCursor> parentHandle;
        private IChainStateCursor parentCursor;
        private ActionBlock<WorkQueueDictionary<UInt256, UnspentTx>.WorkItem> utxoApplier;
        private bool changesApplied;

        public DeferredChainStateCursor(IChainState chainState, IStorageManager storageManager)
        {
            this.chainState = chainState;
            this.storageManager = storageManager;

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
        }

        public void Dispose()
        {
            if (inTransaction)
                RollbackTransaction();

            parentHandle?.Dispose();

            if (!utxoApplier.Completion.IsCompleted)
            {
                // ensure utxo applier has completed, but do not propagate exceptions
                var ex = new ObjectDisposedException("DeferredChainStateCursor");
                ((IDataflowBlock)unspentTxes.WorkQueue).Fault(ex);
                ((IDataflowBlock)utxoApplier).Fault(ex);
                try { utxoApplier.Completion.Wait(); }
                catch (Exception) { }
            }
        }

        public IDataflowBlock[] DataFlowBlocks =>
            new IDataflowBlock[]
            {
                unspentTxes.WorkQueue,
                utxoApplier,
            };

        public int CursorCount => chainState.CursorCount;

        public bool InTransaction => inTransaction;

        public void BeginTransaction(bool readOnly, bool pruneOnly)
        {
            CheckNotInTransaction();

            if (wasInTransaction)
                throw new InvalidOperationException("DeferredChainStateCursor may only be used for a single transaction");
            wasInTransaction = true;

            parentHandle = storageManager.OpenChainStateCursor();
            try
            {
                parentCursor = parentHandle.Item;

                parentCursor.BeginTransaction();
                inTransaction = true;

                this.chainTip = parentCursor.ChainTip;
                this.unspentTxCount = parentCursor.UnspentTxCount;
                this.unspentOutputCount = parentCursor.UnspentOutputCount;
                this.totalTxCount = parentCursor.TotalTxCount;
                this.totalInputCount = parentCursor.TotalInputCount;
                this.totalOutputCount = parentCursor.TotalOutputCount;

                this.readOnly = readOnly;
            }
            catch (Exception ex)
            {
                inTransaction = false;

                // ensure utxo applier has completed, but do not propagate exceptions
                ((IDataflowBlock)unspentTxes.WorkQueue).Fault(ex);
                ((IDataflowBlock)utxoApplier).Fault(ex);
                try { utxoApplier.Completion.Wait(); }
                catch (Exception) { }

                parentHandle.Dispose();

                parentHandle = null;
                parentCursor = null;
                utxoApplier = null;

                throw;
            }
        }

        public void CommitTransaction()
        {
            CommitTransactionAsync().Wait();
        }

        public async Task CommitTransactionAsync()
        {
            CheckTransaction();

            await utxoApplier.Completion;

            if (!readOnly)
                await parentCursor.CommitTransactionAsync();
            else
                parentCursor.RollbackTransaction();
            parentHandle.Dispose();

            inTransaction = false;
        }

        public void RollbackTransaction()
        {
            CheckTransaction();

            // ensure utxo applier has completed, but do not propagate exceptions
            try { utxoApplier.Completion.Wait(); }
            catch (Exception) { }

            parentCursor.RollbackTransaction();
            parentHandle.Dispose();

            inTransaction = false;
        }

        public IEnumerable<ChainedHeader> ReadChain()
        {
            throw new NotSupportedException();
        }

        public ChainedHeader ChainTip
        {
            get
            {
                CheckTransaction();
                return chainTip;
            }
            set
            {
                CheckWriteTransaction();
                chainTip = value;
            }
        }

        public int UnspentTxCount
        {
            get
            {
                CheckTransaction();
                return unspentTxCount;
            }
            set
            {
                CheckWriteTransaction();
                unspentTxCount = value;
            }
        }

        public int UnspentOutputCount
        {
            get
            {
                CheckTransaction();
                return unspentOutputCount;
            }
            set
            {
                CheckWriteTransaction();
                unspentOutputCount = value;
            }
        }

        public int TotalTxCount
        {
            get
            {
                CheckTransaction();
                return totalTxCount;
            }
            set
            {
                CheckWriteTransaction();
                totalTxCount = value;
            }
        }

        public int TotalInputCount
        {
            get
            {
                CheckTransaction();
                return totalInputCount;
            }
            set
            {
                CheckWriteTransaction();
                totalInputCount = value;
            }
        }

        public int TotalOutputCount
        {
            get
            {
                CheckTransaction();
                return totalOutputCount;
            }
            set
            {
                CheckWriteTransaction();
                totalOutputCount = value;
            }
        }

        public bool ContainsHeader(UInt256 blockHash)
        {
            CheckTransaction();
            return headers.ContainsKey(blockHash);
        }

        public bool TryGetHeader(UInt256 blockHash, out ChainedHeader header)
        {
            CheckTransaction();
            return headers.TryGetValue(blockHash, out header);
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
            return unspentTxes.ContainsKey(txHash);
        }

        public bool TryGetUnspentTx(UInt256 txHash, out UnspentTx unspentTx)
        {
            CheckTransaction();
            return unspentTxes.TryGetValue(txHash, out unspentTx);
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
            throw new NotSupportedException();
        }

        public bool ContainsBlockSpentTxes(int blockIndex)
        {
            CheckTransaction();
            return blockSpentTxes.ContainsKey(blockIndex);
        }

        public bool TryGetBlockSpentTxes(int blockIndex, out BlockSpentTxes spentTxes)
        {
            CheckTransaction();
            return blockSpentTxes.TryGetValue(blockIndex, out spentTxes);
        }

        public bool TryAddBlockSpentTxes(int blockIndex, BlockSpentTxes spentTxes)
        {
            CheckWriteTransaction();
            return blockSpentTxes.TryAdd(blockIndex, spentTxes);
        }

        public bool TryRemoveBlockSpentTxes(int blockIndex)
        {
            CheckWriteTransaction();
            return blockSpentTxes.TryRemove(blockIndex);
        }

        public bool ContainsBlockUnmintedTxes(UInt256 blockHash)
        {
            CheckTransaction();
            return blockUnmintedTxes.ContainsKey(blockHash);
        }

        public bool TryGetBlockUnmintedTxes(UInt256 blockHash, out IImmutableList<UnmintedTx> unmintedTxes)
        {
            CheckTransaction();
            return blockUnmintedTxes.TryGetValue(blockHash, out unmintedTxes);
        }

        public bool TryAddBlockUnmintedTxes(UInt256 blockHash, IImmutableList<UnmintedTx> unmintedTxes)
        {
            CheckWriteTransaction();
            return blockUnmintedTxes.TryAdd(blockHash, unmintedTxes);
        }

        public bool TryRemoveBlockUnmintedTxes(UInt256 blockHash)
        {
            CheckWriteTransaction();
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
            unspentTxes.WarmupValue(txHash);
        }

        // TODO - the way this operates is specific to the block validation pipeline, this should be more apparent
        public async Task ApplyChangesAsync()
        {
            CheckWriteTransaction();
            if (changesApplied)
                throw new InvalidOperationException();

            unspentTxes.WorkQueue.Complete();
            await utxoApplier.Completion;

            parentCursor.ChainTip = chainTip;
            parentCursor.UnspentOutputCount = unspentOutputCount;
            parentCursor.UnspentTxCount = unspentTxCount;
            parentCursor.TotalTxCount = totalTxCount;
            parentCursor.TotalInputCount = totalInputCount;
            parentCursor.TotalOutputCount = totalOutputCount;

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

        private void CheckTransaction()
        {
            if (!inTransaction)
                throw new InvalidOperationException();
        }

        private void CheckNotInTransaction()
        {
            if (inTransaction)
                throw new InvalidOperationException();
        }

        private void CheckWriteTransaction()
        {
            if (!inTransaction || readOnly)
                throw new InvalidOperationException();
        }

        public void WarmUnspentTxOutput(TxOutputKey txOutputKey)
        {
            throw new NotImplementedException();
        }

        public bool ContainsUnspentTxOutput(TxOutputKey txOutputKey)
        {
            throw new NotImplementedException();
        }

        public bool TryGetUnspentTxOutput(TxOutputKey txOutputKey, out TxOutput txOutput)
        {
            throw new NotImplementedException();
        }

        public bool TryAddUnspentTxOutput(TxOutputKey txOutputKey, TxOutput txOutput)
        {
            throw new NotImplementedException();
        }

        public bool TryRemoveUnspentTxOutput(TxOutputKey txOutputKey)
        {
            throw new NotImplementedException();
        }

        public void RemoveUnspentTxOutput(TxOutputKey txOutputKey)
        {
            throw new NotImplementedException();
        }
    }
}
