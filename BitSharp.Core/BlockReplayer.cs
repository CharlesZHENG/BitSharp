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

namespace BitSharp.Core.Builders
{
    public class BlockReplayer : IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly CoreStorage coreStorage;
        private readonly IBlockchainRules rules;

        private readonly UtxoReplayer pendingTxLoader;
        private readonly ParallelReader<LoadingTx> loadingTxesSource;
        private readonly ParallelReader<LoadedTx> loadedTxesSource;
        private readonly TxLoader txLoader;
        private readonly ParallelObserver<LoadedTx> txSorter;

        private bool isDisposed;

        public BlockReplayer(CoreStorage coreStorage, IBlockchainRules rules)
        {
            this.coreStorage = coreStorage;
            this.rules = rules;

            // thread count for i/o task (TxLoader)
            var ioThreadCount = 4;

            this.pendingTxLoader = new UtxoReplayer("BlockReplayer", coreStorage, ioThreadCount);
            this.loadingTxesSource = new ParallelReader<LoadingTx>("BlockReplayer.LoadingTxesSource");
            this.loadedTxesSource = new ParallelReader<LoadedTx>("BlockReplayer.LoadedTxesSource");
            this.txLoader = new TxLoader("BlockReplayer", null, coreStorage, ioThreadCount);
            this.txSorter = new ParallelObserver<LoadedTx>("BlockReplayer", 1);
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
                this.loadedTxesSource.Dispose();
                this.txLoader.Dispose();
                this.txSorter.Dispose();

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
                    using (var loadingTxesTask = this.loadingTxesSource.ReadAsync(loadingTxes.GetConsumingEnumerable()).WaitOnDispose())
                    using (var loadedTxesTask = this.loadedTxesSource.ReadAsync(this.txLoader.LoadTxes(loadingTxesSource)).WaitOnDispose())
                    using (var sortedTxes = new ConcurrentBlockingQueue<LoadedTx>())
                    using (var txSorterTask = StartTxSorter(replayBlock, loadedTxesSource, sortedTxes).WaitOnDispose())
                    {
                        var replayTxes = sortedTxes.GetConsumingEnumerable();
                        //TODO Reverse() here means everything must be loaded first, the tx sorter should handle this instead
                        if (!replayForward)
                            replayTxes = replayTxes.Reverse();

                        foreach (var tx in replayTxes)
                            replayAction(tx);
                    }
                });
        }

        private Task StartTxSorter(ChainedHeader replayBlock, ParallelReader<LoadedTx> loadedTxes, ConcurrentBlockingQueue<LoadedTx> sortedTxes)
        {
            // txSorter will only have a single consumer thread, so SortedList is safe to use
            var pendingSortedTxes = new SortedList<int, LoadedTx>();

            // keep track of which tx is the next one in order
            var nextTxIndex = 0;

            return this.txSorter.SubscribeObservers(loadedTxes,
                Observer.Create<LoadedTx>(
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
                    },
                    ex => sortedTxes.CompleteAdding(),
                    () => sortedTxes.CompleteAdding()));
        }
    }
}
