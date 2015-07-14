using BitSharp.Common;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage;
using NLog;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading.Tasks.Dataflow;

namespace BitSharp.Core.Builders
{
    internal class DeferredChainStateCursor : IDeferredChainStateCursor
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly IChainState chainState;
        private readonly ChainedHeader originalChainTip;

        private DeferredDictionary<UInt256, ChainedHeader> headers;
        private WorkQueueDictionary<UInt256, UnspentTx> unspentTxes;
        private DeferredDictionary<int, BlockSpentTxes> blockSpentTxes;
        private DeferredDictionary<UInt256, IImmutableList<UnmintedTx>> blockUnmintedTxes;

        private bool changesApplied;

        public DeferredChainStateCursor(IChainState chainState)
        {
            this.chainState = chainState;
            this.originalChainTip = chainState.Chain.LastBlock;

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
        }

        public int CursorCount { get { return chainState.CursorCount; } }

        public bool InTransaction
        {
            get { throw new NotSupportedException(); }
        }

        public void BeginTransaction(bool readOnly = false)
        {
            throw new NotSupportedException();
        }

        public void CommitTransaction()
        {
            throw new NotSupportedException();
        }

        public void RollbackTransaction()
        {
            throw new NotSupportedException();
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

        public void CompleteWorkQueue()
        {
            unspentTxes.WorkQueue.Complete();
        }

        public void ApplyChangesToParent(IChainStateCursor parent)
        {
            if (changesApplied)
                throw new InvalidOperationException();

            var currentChainTip = parent.ChainTip;
            if (!(currentChainTip == null && originalChainTip == null) && currentChainTip.Hash != originalChainTip.Hash)
                throw new InvalidOperationException();

            var utxoApplier = new ActionBlock<WorkQueueDictionary<UInt256, UnspentTx>.WorkItem>(
                workItem =>
                {
                    workItem.Consume(
                        (operation, unspenTxHash, unspentTx) =>
                        {
                            switch (operation)
                            {
                                case WorkQueueOperation.Nothing:
                                    break;

                                case WorkQueueOperation.Add:
                                    if (!parent.TryAddUnspentTx(unspentTx))
                                        throw new InvalidOperationException();
                                    break;

                                case WorkQueueOperation.Update:
                                    if (!parent.TryUpdateUnspentTx(unspentTx))
                                        throw new InvalidOperationException();
                                    break;

                                case WorkQueueOperation.Remove:
                                    if (!parent.TryRemoveUnspentTx(unspenTxHash))
                                        throw new InvalidOperationException();
                                    break;

                                default:
                                    throw new InvalidOperationException();
                            }
                        });
                });

            unspentTxes.WorkQueue.LinkTo(utxoApplier, new DataflowLinkOptions { PropagateCompletion = true });
            utxoApplier.Completion.Wait();

            parent.ChainTip = ChainTip;
            parent.UnspentOutputCount = UnspentOutputCount;
            parent.UnspentTxCount = UnspentTxCount;
            parent.TotalTxCount = TotalTxCount;
            parent.TotalInputCount = TotalInputCount;
            parent.TotalOutputCount = TotalOutputCount;

            if (blockSpentTxes.Updated.Count > 0)
                throw new InvalidOperationException();
            foreach (var spentTxes in blockSpentTxes.Added)
                if (!parent.TryAddBlockSpentTxes(spentTxes.Key, spentTxes.Value))
                    throw new InvalidOperationException();
            foreach (var blockHeight in blockSpentTxes.Deleted)
                if (!parent.TryRemoveBlockSpentTxes(blockHeight))
                    throw new InvalidOperationException();

            if (blockUnmintedTxes.Updated.Count > 0)
                throw new InvalidOperationException();
            foreach (var unmintedTxes in blockUnmintedTxes.Added)
                if (!parent.TryAddBlockUnmintedTxes(unmintedTxes.Key, unmintedTxes.Value))
                    throw new InvalidOperationException();
            foreach (var blockHeight in blockUnmintedTxes.Deleted)
                if (!parent.TryRemoveBlockUnmintedTxes(blockHeight))
                    throw new InvalidOperationException();

            changesApplied = true;
        }
    }
}
