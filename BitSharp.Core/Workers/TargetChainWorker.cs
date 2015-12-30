using BitSharp.Common;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using NLog;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Core.Workers
{
    internal class TargetChainWorker : Worker
    {
        public event Action OnTargetChainChanged;

        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly IChainParams chainParams;
        private readonly CoreStorage coreStorage;

        private readonly UpdatedTracker updatedTracker = new UpdatedTracker();
        private readonly ManualResetEventSlim inittedEvent = new ManualResetEventSlim();

        private ChainedHeader targetBlock;
        private Chain targetChain;

        public TargetChainWorker(WorkerConfig workerConfig, IChainParams chainParams, CoreStorage coreStorage)
            : base("TargetChainWorker", workerConfig.initialNotify, workerConfig.minIdleTime, workerConfig.maxIdleTime)
        {
            this.chainParams = chainParams;
            this.coreStorage = coreStorage;

            this.coreStorage.ChainedHeaderAdded += HandleChanged;
            this.coreStorage.ChainedHeaderRemoved += HandleChanged;
            this.coreStorage.BlockInvalidated += HandleChanged;
        }

        protected override void SubDispose()
        {
            // cleanup events
            this.coreStorage.ChainedHeaderAdded -= HandleChanged;
            this.coreStorage.ChainedHeaderRemoved -= HandleChanged;
            this.coreStorage.BlockInvalidated -= HandleChanged;

            inittedEvent.Dispose();
        }

        public Chain TargetChain
        {
            get
            {
                if (!inittedEvent.IsSet)
                {
                    NotifyAndStart();
                    inittedEvent.Wait();
                }
                return this.targetChain;
            }
        }

        public void WaitForUpdate()
        {
            this.updatedTracker.WaitForUpdate();
        }

        public bool WaitForUpdate(TimeSpan timeout)
        {
            return this.updatedTracker.WaitForUpdate(timeout);
        }

        public void ForceUpdate()
        {
            updatedTracker.MarkStale();
            ForceWork();
        }

        public void ForceUpdateAndWait()
        {
            ForceUpdate();
            WaitForUpdate();
        }

        protected override Task WorkAction()
        {
            UpdateTargetBlock();
            UpdateTargetChain();
            return Task.FromResult(false);
        }

        private void UpdateTargetBlock()
        {
            var maxTotalWork = this.coreStorage.FindMaxTotalWork();
            if (
                // always update if there is no current target block
                this.targetBlock == null
                // or if the current target block is invalid
                || this.coreStorage.IsBlockInvalid(this.targetBlock.Hash)
                // otherwise, only change the current target if the amount of work differs
                // this is to ensure the current target does not change on a blockchain split and tie
                || this.targetBlock.TotalWork != maxTotalWork.TotalWork)
            {
                this.targetBlock = maxTotalWork;
            }
        }

        private void UpdateTargetChain()
        {
            using (updatedTracker.TryUpdate(staleAction: NotifyWork))
            {
                try
                {
                    var targetBlockLocal = this.targetBlock;
                    var targetChainLocal = this.targetChain;

                    if (targetBlockLocal != null && targetBlockLocal.Hash != targetChainLocal?.LastBlock.Hash)
                    {
                        var newTargetChain = targetChainLocal?.ToBuilder()
                            ?? new ChainBuilder(Chain.CreateForGenesisBlock(this.chainParams.GenesisChainedHeader));

                        var deltaBlockPath = new BlockchainWalker().GetBlockchainPath(newTargetChain.LastBlock, targetBlockLocal, blockHash => this.coreStorage.GetChainedHeader(blockHash));

                        foreach (var rewindBlock in deltaBlockPath.RewindBlocks)
                        {
                            newTargetChain.RemoveBlock(rewindBlock);
                        }

                        foreach (var advanceBlock in deltaBlockPath.AdvanceBlocks)
                        {
                            newTargetChain.AddBlock(advanceBlock);
                        }

                        logger.Debug($"Winning chained block {newTargetChain.LastBlock.Hash} at height {newTargetChain.Height}, total work: {newTargetChain.LastBlock.TotalWork:X}");
                        this.targetChain = newTargetChain.ToImmutable();

                        this.OnTargetChainChanged?.Invoke();
                    }
                }
                catch (MissingDataException) { }
                finally
                {
                    inittedEvent.Set();
                }
            }
        }

        private void HandleChanged()
        {
            updatedTracker.MarkStale();
            this.NotifyWork();
        }

        private void HandleChanged(ChainedHeader chainedHeader)
        {
            HandleChanged();
        }

        private void HandleChanged(UInt256 blockHash)
        {
            HandleChanged();
        }
    }
}
