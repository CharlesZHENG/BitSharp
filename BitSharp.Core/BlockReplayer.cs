using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using NLog;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Reactive;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using BitSharp.Core;
using System.Collections.Concurrent;

namespace BitSharp.Core.Builders
{
    public static class BlockReplayer
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        public static ISourceBlock<LoadedTx> ReplayBlock(ICoreStorage coreStorage, IChainState chainState, UInt256 blockHash, bool replayForward, CancellationToken cancelToken = default(CancellationToken))
        {
            ChainedHeader replayBlock;
            if (!coreStorage.TryGetChainedHeader(blockHash, out replayBlock))
                throw new MissingDataException(blockHash);

            // replay the loading txes for this block, in reverse order for a rollback
            ISourceBlock<LoadingTx> loadingTxes;
            if (replayForward)
                loadingTxes = UtxoReplayer.ReplayCalculateUtxo(coreStorage, chainState, replayBlock, cancelToken);
            else
                loadingTxes = UtxoReplayer.ReplayRollbackUtxo(coreStorage, chainState, replayBlock, cancelToken);

            // capture the original order of the loading txes
            var originalLoadingTxes = new ConcurrentQueue<UInt256>();
            var originalOrderCapturer = new TransformBlock<LoadingTx, LoadingTx>(
                loadingTx =>
                {
                    originalLoadingTxes.Enqueue(loadingTx.Transaction.Hash);
                    return loadingTx;
                },
                new ExecutionDataflowBlockOptions { CancellationToken = cancelToken, SingleProducerConstrained = true });
            loadingTxes.LinkTo(originalOrderCapturer, new DataflowLinkOptions { PropagateCompletion = true });

            // begin loading txes
            var loadedTxes = TxLoader.LoadTxes("", coreStorage, 16, originalOrderCapturer, cancelToken);

            // sort loaded txes
            var pendingLoadedTxes = new Dictionary<UInt256, LoadedTx>();
            var txSorter = new TransformManyBlock<LoadedTx, LoadedTx>(
                loadedTx =>
                {
                    pendingLoadedTxes.Add(loadedTx.Transaction.Hash, loadedTx);

                    // look to see if the next tx in original order has been loaded
                    // if so, dequeue the original tx and then continue looking for the next in order
                    var sortedLoadedTxes = new List<LoadedTx>();
                    bool foundSortedTx;
                    do
                    {
                        foundSortedTx = false;

                        UInt256 nextTxHash;
                        if (originalLoadingTxes.TryPeek(out nextTxHash))
                        {
                            foreach (var pendingLoadedTx in pendingLoadedTxes.Values)
                            {
                                // found a loaded tx for the next tx thas is in original order
                                if (pendingLoadedTx.Transaction.Hash == nextTxHash)
                                {
                                    sortedLoadedTxes.Add(pendingLoadedTx);
                                    pendingLoadedTxes.Remove(nextTxHash);
                                    originalLoadingTxes.TryDequeue(out nextTxHash);

                                    foundSortedTx = true;
                                    break;
                                }
                            }
                        }
                    }
                    while (foundSortedTx);

                    return sortedLoadedTxes;
                },
                new ExecutionDataflowBlockOptions { CancellationToken = cancelToken, SingleProducerConstrained = true });
            loadedTxes.LinkTo(txSorter, new DataflowLinkOptions { PropagateCompletion = true });

            // return replay txes
            return txSorter;
        }
    }
}
