using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using NLog;
using System;
using System.Threading;

namespace BitSharp.Core.Workers
{
    internal class TargetChainWorker : Worker
    {
        public event Action OnTargetChainChanged;

        private readonly Logger logger;
        private readonly IBlockchainRules rules;
        private readonly CoreStorage coreStorage;

        private readonly ManualResetEventSlim updatedEvent = new ManualResetEventSlim();
        private readonly object changedLock = new object();
        private long changed;

        private ChainedHeader targetBlock;
        private Chain targetChain;

        public TargetChainWorker(WorkerConfig workerConfig, Logger logger, IBlockchainRules rules, CoreStorage coreStorage)
            : base("TargetChainWorker", workerConfig.initialNotify, workerConfig.minIdleTime, workerConfig.maxIdleTime, logger)
        {
            this.logger = logger;
            this.rules = rules;
            this.coreStorage = coreStorage;

            this.coreStorage.ChainedHeaderAdded += HandleChainedHeaderAdded;
            this.coreStorage.BlockInvalidated += HandleBlockInvalidated;
        }

        protected override void SubDispose()
        {
            // cleanup events
            this.coreStorage.ChainedHeaderAdded -= HandleChainedHeaderAdded;
            this.coreStorage.BlockInvalidated -= HandleBlockInvalidated;
        }

        public Chain TargetChain { get { return this.targetChain; } }

        public void WaitForUpdate()
        {
            this.updatedEvent.Wait();
        }

        public bool WaitForUpdate(TimeSpan timeout)
        {
            return this.updatedEvent.Wait(timeout);
        }

        protected override void WorkAction()
        {
            UpdateTargetBlock();
            UpdateTargetChain();
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
            try
            {
                long origChanged;
                lock (this.changedLock)
                    origChanged = this.changed;

                var targetBlockLocal = this.targetBlock;
                var targetChainLocal = this.targetChain;

                if (targetBlockLocal != null &&
                    (targetChainLocal == null || targetBlockLocal.Hash != targetChainLocal.LastBlock.Hash))
                {
                    var newTargetChain =
                        targetChainLocal != null
                            ? targetChainLocal.ToBuilder()
                            : new ChainBuilder(Chain.CreateForGenesisBlock(this.rules.GenesisChainedHeader));

                    var deltaBlockPath = new BlockchainWalker().GetBlockchainPath(newTargetChain.LastBlock, targetBlockLocal, blockHash => this.coreStorage.GetChainedHeader(blockHash));

                    foreach (var rewindBlock in deltaBlockPath.RewindBlocks)
                    {
                        newTargetChain.RemoveBlock(rewindBlock);
                    }

                    foreach (var advanceBlock in deltaBlockPath.AdvanceBlocks)
                    {
                        newTargetChain.AddBlock(advanceBlock);
                    }

                    this.logger.Debug("Winning chained block {0} at height {1}, total work: {2}".Format2(newTargetChain.LastBlock.Hash.ToHexNumberString(), newTargetChain.Height, newTargetChain.LastBlock.TotalWork.ToString("X")));
                    this.targetChain = newTargetChain.ToImmutable();

                    var handler = this.OnTargetChainChanged;
                    if (handler != null)
                        handler();
                }

                lock (this.changedLock)
                {
                    if (this.changed == origChanged)
                        this.updatedEvent.Set();
                    else
                        this.NotifyWork();
                }
            }
            catch (MissingDataException) { }
        }

        private void HandleChanged()
        {
            lock (this.changedLock)
            {
                this.changed++;
                this.updatedEvent.Reset();
            }
            this.NotifyWork();
        }

        private void HandleChainedHeaderAdded(ChainedHeader chainedHeader)
        {
            HandleChanged();
        }

        private void HandleBlockInvalidated(UInt256 blockHash)
        {
            HandleChanged();
        }
    }
}
