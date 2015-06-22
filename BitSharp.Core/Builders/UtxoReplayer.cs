using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using NLog;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Collections.Immutable;
using BitSharp.Core.Storage;
using System.Reactive;
using System.Threading.Tasks;

namespace BitSharp.Core.Builders
{
    internal class UtxoReplayer : IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly CoreStorage coreStorage;

        private readonly ParallelReader<BlockTx> blockTxesSource;
        private readonly ParallelObserver<BlockTx> loadingTxLookuper;

        public UtxoReplayer(string name, CoreStorage coreStorage, int threadCount)
        {
            this.coreStorage = coreStorage;

            this.blockTxesSource = new ParallelReader<BlockTx>(name + ".BlockTxesSource");
            this.loadingTxLookuper = new ParallelObserver<BlockTx>(name + ".PendingTxLoader", threadCount);
        }

        public void Dispose()
        {
            this.blockTxesSource.Dispose();
            this.loadingTxLookuper.Dispose();
        }

        public void ReplayCalculateUtxo(IChainState chainState, ChainedHeader replayBlock, bool replayForward, Action<ConcurrentBlockingQueue<LoadingTx>> workAction)
        {
            using (var loadingTxes = new ConcurrentBlockingQueue<LoadingTx>())
            {
                IEnumerable<BlockTx> blockTxes;
                if (!this.coreStorage.TryReadBlockTransactions(replayBlock.Hash, replayBlock.MerkleRoot, /*requireTransaction:*/true, out blockTxes))
                {
                    throw new MissingDataException(replayBlock.Hash);
                }

                if (replayForward)
                {
                    using (var blockTxesTask = this.blockTxesSource.ReadAsync(blockTxes).WaitOnDispose())
                    //TODO replay backwards should also use this code path if this is not a re-org block, there won't be unminted txes
                    //TODO should be a ReplayRollbackUtxo method
                    using (var loadingTxLookupTask = StartLoadingTxLookup(chainState, replayBlock, blockTxesSource, loadingTxes).WaitOnDispose())
                    {
                        workAction(loadingTxes);
                    }
                }
                else
                {
                    IImmutableList<UnmintedTx> unmintedTxesList;
                    if (!chainState.TryGetBlockUnmintedTxes(replayBlock.Hash, out unmintedTxesList))
                        throw new MissingDataException(replayBlock.Hash);

                    var unmintedTxes = ImmutableDictionary.CreateRange(
                        unmintedTxesList.Select(x => new KeyValuePair<UInt256, UnmintedTx>(x.TxHash, x)));

                    foreach (var blockTx in blockTxes)
                    {
                        var tx = blockTx.Transaction;
                        var txIndex = blockTx.Index;
                        var prevOutputTxKeys = ImmutableArray.CreateBuilder<TxLookupKey>(!blockTx.IsCoinbase ? tx.Inputs.Length : 0);

                        if (!blockTx.IsCoinbase)
                        {
                            UnmintedTx unmintedTx;
                            if (!unmintedTxes.TryGetValue(tx.Hash, out unmintedTx))
                                throw new MissingDataException(replayBlock.Hash);

                            prevOutputTxKeys.AddRange(unmintedTx.PrevOutputTxKeys);
                        }

                        loadingTxes.Add(new LoadingTx(txIndex, tx, replayBlock, prevOutputTxKeys.MoveToImmutable()));
                    }
                    loadingTxes.CompleteAdding();

                    workAction(loadingTxes);
                }
            }
        }

        private Task StartLoadingTxLookup(IChainState chainState, ChainedHeader replayBlock, ParallelReader<BlockTx> blockTxes, ConcurrentBlockingQueue<LoadingTx> loadingTxes)
        {
            return this.loadingTxLookuper.SubscribeObservers(blockTxes,
                Observer.Create<BlockTx>(
                    blockTx =>
                    {
                        var loadingTx = LookupLoadingTx(chainState, replayBlock, blockTx);
                        if (loadingTx != null)
                            loadingTxes.Add(loadingTx);
                    },
                    ex => loadingTxes.CompleteAdding(),
                    () => loadingTxes.CompleteAdding()));
        }

        private LoadingTx LookupLoadingTx(IChainState chainState, ChainedHeader replayBlock, BlockTx blockTx)
        {
            var tx = blockTx.Transaction;
            var txIndex = blockTx.Index;

            var prevOutputTxKeys = ImmutableArray.CreateBuilder<TxLookupKey>(!blockTx.IsCoinbase ? tx.Inputs.Length : 0);

            if (!blockTx.IsCoinbase)
            {
                for (var inputIndex = 0; inputIndex < tx.Inputs.Length; inputIndex++)
                {
                    var input = tx.Inputs[inputIndex];

                    UnspentTx unspentTx;
                    if (!chainState.TryGetUnspentTx(input.PreviousTxOutputKey.TxHash, out unspentTx))
                        throw new MissingDataException(replayBlock.Hash);

                    var prevOutputBlockHash = chainState.Chain.Blocks[unspentTx.BlockIndex].Hash;
                    var prevOutputTxIndex = unspentTx.TxIndex;

                    prevOutputTxKeys.Add(new TxLookupKey(prevOutputBlockHash, prevOutputTxIndex));
                }
            }

            return new LoadingTx(txIndex, tx, replayBlock, prevOutputTxKeys.MoveToImmutable());
        }
    }
}
