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
using System.Threading;

namespace BitSharp.Core.Builders
{
    public class BlockReplayer : IDisposable
    {
        private readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly CoreStorage coreStorage;
        private readonly IBlockchainRules rules;

        private readonly PendingTxLoader pendingTxLoader;
        private readonly PrevTxLoader prevTxLoader;
        private readonly ParallelConsumer<LoadedTx> txSorter;

        public BlockReplayer(CoreStorage coreStorage, IBlockchainRules rules)
        {
            this.coreStorage = coreStorage;
            this.rules = rules;

            // thread count for i/o task (TxLoader)
            var ioThreadCount = 4;

            this.pendingTxLoader = new PendingTxLoader("BlockReplayer", ioThreadCount);
            this.prevTxLoader = new PrevTxLoader("BlockReplayer", null, coreStorage, ioThreadCount);
            this.txSorter = new ParallelConsumer<LoadedTx>("BlockReplayer", 1);
        }

        public void Dispose()
        {
            this.pendingTxLoader.Dispose();
            this.prevTxLoader.Dispose();
            this.txSorter.Dispose();
        }

        public IEnumerable<LoadedTx> ReplayBlock(IChainState chainState, UInt256 blockHash, bool replayForward)
        {
            var replayTxes = new List<LoadedTx>();
            ReplayBlock(chainState, blockHash, replayForward, loadedTx => replayTxes.Add(loadedTx));
            return replayTxes;
        }

        public void ReplayBlock(IChainState chainState, UInt256 blockHash, bool replayForward, Action<LoadedTx> replayAction)
        {
            var replayBlock = this.coreStorage.GetChainedHeader(blockHash);
            if (replayBlock == null)
                throw new MissingDataException(blockHash);

            ImmutableDictionary<UInt256, UnmintedTx> unmintedTxes;
            if (replayForward)
            {
                unmintedTxes = null;
            }
            else
            {
                IImmutableList<UnmintedTx> unmintedTxesList;
                if (!chainState.TryGetBlockUnmintedTxes(replayBlock.Hash, out unmintedTxesList))
                {
                    throw new MissingDataException(replayBlock.Hash);
                }

                unmintedTxes = ImmutableDictionary.CreateRange(
                    unmintedTxesList.Select(x => new KeyValuePair<UInt256, UnmintedTx>(x.TxHash, x)));
            }

            IEnumerable<BlockTx> blockTxes;
            if (!this.coreStorage.TryReadBlockTransactions(replayBlock.Hash, replayBlock.MerkleRoot, /*requireTransaction:*/true, out blockTxes))
            {
                throw new MissingDataException(replayBlock.Hash);
            }

            using (var sortedTxQueue = new ConcurrentBlockingQueue<LoadedTx>())
            using (this.pendingTxLoader.StartLoading(chainState, replayBlock, replayForward, blockTxes, unmintedTxes))
            using (this.prevTxLoader.StartLoading(this.pendingTxLoader.GetQueue()))
            using (StartTxSorter(replayBlock, this.prevTxLoader.GetQueue(), sortedTxQueue))
            {
                var replayTxes = sortedTxQueue.GetConsumingEnumerable();
                //TODO Reverse() here means everything must be loaded first, the tx sorter should handle this instead
                if (!replayForward)
                    replayTxes = replayTxes.Reverse();

                foreach (var tx in replayTxes)
                    replayAction(tx);

                // wait for loaders to finish, any exceptions will be rethrown here
                pendingTxLoader.WaitToComplete();
                prevTxLoader.WaitToComplete();
                txSorter.WaitToComplete();
            }
        }

        private IDisposable StartTxSorter(ChainedHeader replayBlock, ConcurrentBlockingQueue<LoadedTx> loadedTxQueue, ConcurrentBlockingQueue<LoadedTx> sortedTxQueue)
        {
            // txSorter will only have a single consumer thread, so SortedList is safe to use
            var sortedTxes = new SortedList<int, LoadedTx>();

            // keep track of which tx is the next one in order
            var nextTxIndex = 0;

            return this.txSorter.Start(loadedTxQueue,
                loadedTx =>
                {
                    // store loaded tx
                    sortedTxes.Add(loadedTx.TxIndex, loadedTx);

                    // dequeue any available loaded txes that are in order
                    while (sortedTxes.Count > 0 && sortedTxes.Keys[0] == nextTxIndex)
                    {
                        sortedTxQueue.Add(sortedTxes.Values[0]);
                        sortedTxes.RemoveAt(0);
                        nextTxIndex++;
                    }
                },
                e =>
                {
                    // ensure no txes were left unsorted
                    if (sortedTxes.Count > 0)
                        throw new InvalidOperationException();

                    sortedTxQueue.CompleteAdding();
                });
        }
    }
}
