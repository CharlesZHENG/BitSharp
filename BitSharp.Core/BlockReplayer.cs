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
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BitSharp.Core.Builders
{
    public class BlockReplayer : IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly CoreStorage coreStorage;
        private readonly IBlockchainRules rules;

        private readonly UtxoReplayer pendingTxLoader;
        private readonly ParallelReader<LoadingTx> loadingTxesSource;

        private bool isDisposed;

        public BlockReplayer(CoreStorage coreStorage, IBlockchainRules rules)
        {
            this.coreStorage = coreStorage;
            this.rules = rules;

            // thread count for i/o task (TxLoader)
            var ioThreadCount = 4;

            this.pendingTxLoader = new UtxoReplayer("BlockReplayer", coreStorage, ioThreadCount);
            this.loadingTxesSource = new ParallelReader<LoadingTx>("BlockReplayer.LoadingTxesSource");
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!isDisposed && disposing)
            {
                this.pendingTxLoader.Dispose();
                this.loadingTxesSource.Dispose();

                isDisposed = true;
            }
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

            this.pendingTxLoader.ReplayCalculateUtxo(chainState, replayBlock, replayForward,
                loadingTxes =>
                {
                    var loadingTxesBuffer = new BufferBlock<LoadingTx>();
                    var txLoader = TxLoader.LoadTxes("BlockReplayer", coreStorage, 4, loadingTxesBuffer);

                    using (var loadingTxesTask = this.loadingTxesSource.ReadAsync(loadingTxes.GetConsumingEnumerable()).WaitOnDispose())
                    using (var sortedTxes = new ConcurrentBlockingQueue<LoadedTx>())
                    using (var txSorterTask = StartTxSorter(replayBlock, txLoader, sortedTxes).WaitOnDispose())
                    {
                        foreach (var loadingTx in loadingTxesSource.GetConsumingEnumerable())
                            loadingTxesBuffer.Post(loadingTx);
                        loadingTxesBuffer.Complete();

                        var replayTxes = sortedTxes.GetConsumingEnumerable();
                        //TODO Reverse() here means everything must be loaded first, the tx sorter should handle this instead
                        if (!replayForward)
                            replayTxes = replayTxes.Reverse();

                        foreach (var tx in replayTxes)
                            replayAction(tx);
                    }
                });
        }

        private async Task StartTxSorter(ChainedHeader replayBlock, ISourceBlock<LoadedTx> loadedTxes, ConcurrentBlockingQueue<LoadedTx> sortedTxes)
        {
            // txSorter will only have a single consumer thread, so SortedList is safe to use
            var pendingSortedTxes = new SortedList<int, LoadedTx>();

            // keep track of which tx is the next one in order
            var nextTxIndex = 0;

            var txSorter = new ActionBlock<LoadedTx>(
                loadedTx =>
                {
                    // store loaded tx
                    pendingSortedTxes.Add(loadedTx.TxIndex, loadedTx);

                    // dequeue any available loaded txes that are in order
                    while (pendingSortedTxes.Count > 0 && pendingSortedTxes.Keys[0] == nextTxIndex)
                    {
                        sortedTxes.Add(pendingSortedTxes.Values[0]);
                        pendingSortedTxes.RemoveAt(0);
                        nextTxIndex++;
                    }
                });

            loadedTxes.LinkTo(txSorter, new DataflowLinkOptions { PropagateCompletion = true });

            try
            {
                await txSorter.Completion;
            }
            finally
            {
                sortedTxes.CompleteAdding();
            }
        }
    }
}
