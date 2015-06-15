using BitSharp.Common;
using BitSharp.Core.Domain;
using NLog;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace BitSharp.Core.Builders
{
    internal class PendingTxLoader : IDisposable
    {
        private readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly LookAhead<BlockTx> blockTxesLookAhead;
        private readonly ParallelConsumer<BlockTx> pendingTxLoader;

        private IChainState chainState;
        private ChainedHeader replayBlock;
        private bool replayForward;
        private ImmutableDictionary<UInt256, UnmintedTx> unmintedTxes;

        private ConcurrentBlockingQueue<TxWithInputTxLookupKeys> pendingTxes;
        private IDisposable txLoaderStopper;

        public PendingTxLoader(string name, int threadCount)
        {
            this.blockTxesLookAhead = new LookAhead<BlockTx>(name + ".BlockTxesLookAhead");
            this.pendingTxLoader = new ParallelConsumer<BlockTx>(name + ".PendingTxLoader", threadCount);
        }

        public void Dispose()
        {
            this.pendingTxLoader.Dispose();

            if (this.pendingTxes != null)
                this.pendingTxes.Dispose();

            this.blockTxesLookAhead.Dispose();
        }

        public int PendingCount { get { return this.pendingTxLoader.PendingCount; } }

        public IDisposable StartLoading(IChainState chainState, ChainedHeader replayBlock, bool replayForward, IEnumerable<BlockTx> blockTxes, ImmutableDictionary<UInt256, UnmintedTx> unmintedTxes)
        {
            this.chainState = chainState;
            this.replayBlock = replayBlock;
            this.replayForward = replayForward;
            this.unmintedTxes = unmintedTxes;

            this.pendingTxes = new ConcurrentBlockingQueue<TxWithInputTxLookupKeys>();

            this.txLoaderStopper = StartPendingTxLoader(blockTxes);

            return new DisposeAction(StopLoading);
        }

        public void WaitToComplete()
        {
            this.pendingTxLoader.WaitToComplete();
        }

        public ConcurrentBlockingQueue<TxWithInputTxLookupKeys> GetQueue()
        {
            return this.pendingTxes;
        }

        private void StopLoading()
        {
            this.pendingTxes.CompleteAdding();

            this.txLoaderStopper.Dispose();
            this.pendingTxes.Dispose();

            this.chainState = null;
            this.replayBlock = null;
            this.unmintedTxes = null;

            this.pendingTxes = null;
            this.txLoaderStopper = null;
        }

        private IDisposable StartPendingTxLoader(IEnumerable<BlockTx> blockTxes)
        {
            return this.pendingTxLoader.Start(blockTxesLookAhead.ReadAll(blockTxes),
                blockTx =>
                {
                    var pendingTx = LoadPendingTx(blockTx);
                    if (pendingTx != null)
                        this.pendingTxes.Add(pendingTx);
                },
                _ => this.pendingTxes.CompleteAdding());
        }

        private TxWithInputTxLookupKeys LoadPendingTx(BlockTx blockTx)
        {
            var tx = blockTx.Transaction;
            var txIndex = blockTx.Index;

            var prevOutputTxKeys = ImmutableArray.CreateBuilder<TxLookupKey>(txIndex > 0 ? tx.Inputs.Length : 0);

            if (txIndex > 0)
            {
                if (this.replayForward)
                {
                    for (var inputIndex = 0; inputIndex < tx.Inputs.Length; inputIndex++)
                    {
                        var input = tx.Inputs[inputIndex];

                        UnspentTx unspentTx;
                        if (!this.chainState.TryGetUnspentTx(input.PreviousTxOutputKey.TxHash, out unspentTx))
                            throw new MissingDataException(this.replayBlock.Hash);

                        var prevOutputBlockHash = this.chainState.Chain.Blocks[unspentTx.BlockIndex].Hash;
                        var prevOutputTxIndex = unspentTx.TxIndex;

                        prevOutputTxKeys.Add(new TxLookupKey(prevOutputBlockHash, prevOutputTxIndex));
                    }
                }
                else
                {
                    UnmintedTx unmintedTx;
                    if (!this.unmintedTxes.TryGetValue(tx.Hash, out unmintedTx))
                        throw new MissingDataException(this.replayBlock.Hash);

                    prevOutputTxKeys.AddRange(unmintedTx.PrevOutputTxKeys);
                }
            }

            var pendingTx = new TxWithInputTxLookupKeys(txIndex, tx, this.replayBlock, prevOutputTxKeys.MoveToImmutable());
            return pendingTx;
        }
    }
}
