using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;

namespace BitSharp.Core.Builders
{
    public class BlockReplayer : IDisposable
    {
        private readonly Logger logger;
        private readonly CoreStorage coreStorage;
        private readonly IBlockchainRules rules;

        private readonly ParallelConsumer<BlockTx> pendingTxLoader;
        private readonly ParallelConsumer<TxWithPrevOutputKeys> txLoader;

        private IChainState chainState;
        private ChainedHeader replayBlock;
        private bool replayForward;
        private ImmutableDictionary<UInt256, UnmintedTx> unmintedTxes;
        private ConcurrentDictionary<UInt256, Transaction> txCache;
        private ConcurrentBlockingQueue<TxWithPrevOutputKeys> pendingTxes;
        private ConcurrentBlockingQueue<TxWithPrevOutputs> loadedTxes;
        private IDisposable pendingTxLoaderStopper;
        private IDisposable txLoaderStopper;
        private ConcurrentBag<Exception> pendingTxLoaderExceptions;
        private ConcurrentBag<Exception> txLoaderExceptions;

        public BlockReplayer(CoreStorage coreStorage, IBlockchainRules rules, Logger logger)
        {
            this.logger = logger;
            this.coreStorage = coreStorage;
            this.rules = rules;

            // thread count for i/o task (TxLoader)
            var ioThreadCount = Environment.ProcessorCount * 8;

            this.pendingTxLoader = new ParallelConsumer<BlockTx>("BlockReplayer.PendingTxLoader", ioThreadCount, logger);
            this.txLoader = new ParallelConsumer<TxWithPrevOutputKeys>("BlockReplayer.TxLoader", ioThreadCount, logger);
        }

        public void Dispose()
        {
            this.pendingTxLoader.Dispose();
            this.txLoader.Dispose();

            if (this.pendingTxes != null)
                this.pendingTxes.Dispose();

            if (this.loadedTxes != null)
                this.loadedTxes.Dispose();
        }

        public IDisposable StartReplay(IChainState chainState, UInt256 blockHash)
        {
            this.chainState = chainState;
            this.replayBlock = this.coreStorage.GetChainedHeader(blockHash);

            if (chainState.Chain.BlocksByHash.ContainsKey(replayBlock.Hash))
            {
                this.replayForward = true;
            }
            else
            {
                this.replayForward = false;

                IImmutableList<UnmintedTx> unmintedTxesList;
                if (!this.chainState.TryGetBlockUnmintedTxes(this.replayBlock.Hash, out unmintedTxesList))
                {
                    throw new MissingDataException(this.replayBlock.Hash);
                }

                this.unmintedTxes = ImmutableDictionary.CreateRange(
                    unmintedTxesList.Select(x => new KeyValuePair<UInt256, UnmintedTx>(x.TxHash, x)));
            }

            IEnumerable<BlockTx> blockTxes;
            if (!this.coreStorage.TryReadBlockTransactions(this.replayBlock.Hash, this.replayBlock.MerkleRoot, /*requireTransaction:*/true, out blockTxes))
            {
                throw new MissingDataException(this.replayBlock.Hash);
            }

            this.txCache = new ConcurrentDictionary<UInt256, Transaction>();

            this.pendingTxes = new ConcurrentBlockingQueue<TxWithPrevOutputKeys>();
            this.loadedTxes = new ConcurrentBlockingQueue<TxWithPrevOutputs>();

            this.pendingTxLoaderExceptions = new ConcurrentBag<Exception>();
            this.txLoaderExceptions = new ConcurrentBag<Exception>();

            this.pendingTxLoaderStopper = StartPendingTxLoader(blockTxes);
            this.txLoaderStopper = StartTxLoader();

            return new Stopper(this);
        }

        //public IDisposable StartReplayRollback(IChainState chainState, UInt256 blockHash)
        //{
        //}

        //TODO result should indicate whether block was played forwards or rolled back
        public IEnumerable<TxWithPrevOutputs> ReplayBlock()
        {
            foreach (var tx in this.loadedTxes.GetConsumingEnumerable())
            {
                // fail early if there are any errors
                this.ThrowIfFailed();

                yield return tx;
            }

            // wait for loaders to finish
            this.pendingTxLoader.WaitToComplete();
            this.txLoader.WaitToComplete();

            // ensure any errors that occurred are thrown
            this.ThrowIfFailed();
        }

        private void ThrowIfFailed()
        {
            if (this.pendingTxLoaderExceptions.Count > 0)
                throw new AggregateException(this.pendingTxLoaderExceptions);

            if (this.txLoaderExceptions.Count > 0)
                throw new AggregateException(this.txLoaderExceptions);
        }

        private void StopReplay()
        {
            this.pendingTxes.CompleteAdding();
            this.loadedTxes.CompleteAdding();

            this.pendingTxLoaderStopper.Dispose();
            this.txLoaderStopper.Dispose();
            this.pendingTxes.Dispose();
            this.loadedTxes.Dispose();

            this.chainState = null;
            this.replayBlock = null;
            this.unmintedTxes = null;
            this.txCache = null;
            this.pendingTxes = null;
            this.loadedTxes = null;
            this.pendingTxLoaderStopper = null;
            this.txLoaderStopper = null;
            this.pendingTxLoaderExceptions = null;
            this.txLoaderExceptions = null;
        }

        private IDisposable StartPendingTxLoader(IEnumerable<BlockTx> blockTxes)
        {
            return this.pendingTxLoader.Start(blockTxes.LookAhead(10),
                blockTx =>
                {
                    var pendingTx = LoadPendingTx(blockTx);
                    if (pendingTx != null)
                        this.pendingTxes.Add(pendingTx);
                },
                () => this.pendingTxes.CompleteAdding());
        }

        private IDisposable StartTxLoader()
        {
            return this.txLoader.Start(this.pendingTxes,
                pendingTx =>
                {
                    var loadedTx = LoadPendingTx(pendingTx, txCache);
                    if (loadedTx != null)
                        this.loadedTxes.Add(loadedTx);
                },
                () => this.loadedTxes.CompleteAdding());
        }

        //TODO conflicting names
        private TxWithPrevOutputKeys LoadPendingTx(BlockTx blockTx)
        {
            try
            {
                var tx = blockTx.Transaction;
                var txIndex = blockTx.Index;

                var prevOutputTxKeys = ImmutableArray.CreateBuilder<BlockTxKey>(tx.Inputs.Length);

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

                            prevOutputTxKeys.Add(new BlockTxKey(prevOutputBlockHash, prevOutputTxIndex));
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

                var pendingTx = new TxWithPrevOutputKeys(txIndex, tx, this.replayBlock, prevOutputTxKeys.ToImmutable());
                return pendingTx;
            }
            catch (Exception e)
            {
                this.pendingTxLoaderExceptions.Add(e);
                //TODO
                return null;
            }
        }

        private TxWithPrevOutputs LoadPendingTx(TxWithPrevOutputKeys pendingTx, ConcurrentDictionary<UInt256, Transaction> txCache)
        {
            try
            {
                var txIndex = pendingTx.TxIndex;
                var transaction = pendingTx.Transaction;
                var chainedHeader = pendingTx.ChainedHeader;
                var spentTxes = pendingTx.PrevOutputTxKeys;

                var prevTxOutputs = ImmutableArray.CreateBuilder<TxOutput>(transaction.Inputs.Length);

                // load previous transactions for each input, unless this is a coinbase transaction
                if (txIndex > 0)
                {
                    for (var inputIndex = 0; inputIndex < transaction.Inputs.Length; inputIndex++)
                    {
                        var input = transaction.Inputs[inputIndex];

                        Transaction cachedPrevTx;
                        if (txCache.TryGetValue(input.PreviousTxOutputKey.TxHash, out cachedPrevTx))
                        {
                            var prevTxOutput = cachedPrevTx.Outputs[input.PreviousTxOutputKey.TxOutputIndex.ToIntChecked()];
                            prevTxOutputs.Add(prevTxOutput);
                        }
                        else
                        {
                            var spentTx = spentTxes[inputIndex];

                            Transaction prevTx;
                            if (this.coreStorage.TryGetTransaction(spentTx.BlockHash, spentTx.TxIndex, out prevTx))
                            {
                                if (input.PreviousTxOutputKey.TxHash != prevTx.Hash)
                                    throw new Exception("TODO");

                                txCache.TryAdd(prevTx.Hash, prevTx);

                                var prevTxOutput = prevTx.Outputs[input.PreviousTxOutputKey.TxOutputIndex.ToIntChecked()];
                                prevTxOutputs.Add(prevTxOutput);
                            }
                            else
                            {
                                throw new Exception("TODO");
                            }
                        }
                    }

                    Debug.Assert(prevTxOutputs.Count == transaction.Inputs.Length);
                }

                var txWithPrevOutputs = new TxWithPrevOutputs(txIndex, transaction, chainedHeader, prevTxOutputs.ToImmutableArray());
                return txWithPrevOutputs;
            }
            catch (Exception e)
            {
                this.txLoaderExceptions.Add(e);
                //TODO
                return null;
            }
        }

        private sealed class Stopper : IDisposable
        {
            private readonly BlockReplayer blockReplayer;

            public Stopper(BlockReplayer blockReplayer)
            {
                this.blockReplayer = blockReplayer;
            }

            public void Dispose()
            {
                this.blockReplayer.StopReplay();
            }
        }
    }
}
