using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using NLog;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace BitSharp.Core.Workers
{
    internal class ChainStateWorker : Worker
    {
        public event Action OnChainStateChanged;

        private readonly Logger logger;
        private readonly IBlockchainRules rules;
        private readonly CoreStorage coreStorage;

        private readonly ManualResetEventSlim updatedEvent = new ManualResetEventSlim();
        private readonly object changedLock = new object();
        private long changed;

        private readonly DurationMeasure blockProcessingDurationMeasure;
        private readonly CountMeasure blockMissCountMeasure;
        private UInt256? lastBlockMissHash;

        private readonly TargetChainWorker targetChainWorker;
        private readonly ChainStateBuilder chainStateBuilder;
        private Chain currentChain;

        public ChainStateWorker(WorkerConfig workerConfig, TargetChainWorker targetChainWorker, ChainStateBuilder chainStateBuilder, Logger logger, IBlockchainRules rules, CoreStorage coreStorage)
            : base("ChainStateWorker", workerConfig.initialNotify, workerConfig.minIdleTime, workerConfig.maxIdleTime, logger)
        {
            this.logger = logger;
            this.rules = rules;
            this.coreStorage = coreStorage;

            this.blockProcessingDurationMeasure = new DurationMeasure(sampleCutoff: TimeSpan.FromMinutes(5));
            this.blockMissCountMeasure = new CountMeasure(TimeSpan.FromSeconds(30));

            this.targetChainWorker = targetChainWorker;
            this.chainStateBuilder = chainStateBuilder;
            this.currentChain = this.chainStateBuilder.Chain;

            this.coreStorage.BlockInvalidated += HandleChanged;
            this.coreStorage.BlockTxesAdded += HandleChanged;
            this.coreStorage.BlockTxesMissed += HandleChanged;
            this.coreStorage.BlockTxesRemoved += HandleChanged;
            this.coreStorage.ChainedHeaderAdded += HandleChanged;
            this.targetChainWorker.OnTargetChainChanged += HandleChanged;
        }

        public event Action<UInt256> BlockMissed;

        public TimeSpan AverageBlockProcessingTime()
        {
            return this.blockProcessingDurationMeasure.GetAverage();
        }

        public int GetBlockMissCount()
        {
            return this.blockMissCountMeasure.GetCount();
        }

        public Chain CurrentChain
        {
            get { return this.currentChain; }
        }

        public void WaitForUpdate()
        {
            this.updatedEvent.Wait();
        }

        public bool WaitForUpdate(TimeSpan timeout)
        {
            return this.updatedEvent.Wait(timeout);
        }

        protected override void SubDispose()
        {
            this.coreStorage.BlockInvalidated -= HandleChanged;
            this.coreStorage.BlockTxesAdded -= HandleChanged;
            this.coreStorage.BlockTxesMissed -= HandleChanged;
            this.coreStorage.BlockTxesRemoved -= HandleChanged;
            this.coreStorage.ChainedHeaderAdded -= HandleChanged;
            this.targetChainWorker.OnTargetChainChanged -= HandleChanged;

            this.blockProcessingDurationMeasure.Dispose();
            this.blockMissCountMeasure.Dispose();
        }

        protected override void WorkAction()
        {
            try
            {
                long origChanged;
                lock (this.changedLock)
                    origChanged = this.changed;

                // calculate the new blockchain along the target path
                var didWork = false;
                foreach (var pathElement in this.chainStateBuilder.Chain.NavigateTowards(() => this.targetChainWorker.TargetChain))
                {
                    // cooperative loop
                    this.ThrowIfCancelled();

                    // get block and metadata for next link in blockchain
                    var direction = pathElement.Item1;
                    var chainedHeader = pathElement.Item2;
                    IEnumerable<BlockTx> blockTxes;
                    if (!this.coreStorage.TryReadBlockTransactions(chainedHeader.Hash, chainedHeader.MerkleRoot, /*requireTransaction:*/true, out blockTxes))
                    {
                        RaiseBlockMissed(chainedHeader.Hash);
                        break;
                    }

                    didWork = true;

                    var blockStopwatch = Stopwatch.StartNew();
                    if (direction > 0)
                    {
                        this.chainStateBuilder.AddBlock(chainedHeader, blockTxes.LookAhead(10));
                    }
                    else if (direction < 0)
                    {
                        this.logger.Info("Rolling back block {0:#,##0}: {1}".Format2(chainedHeader.Height, chainedHeader.Hash));
                        this.chainStateBuilder.RollbackBlock(chainedHeader, blockTxes.LookAhead(10));
                    }
                    else
                    {
                        Debugger.Break();
                        throw new InvalidOperationException();
                    }
                    blockStopwatch.Stop();
                    this.blockProcessingDurationMeasure.Tick(blockStopwatch.Elapsed);

                    this.currentChain = this.chainStateBuilder.Chain;

                    var handler = this.OnChainStateChanged;
                    if (handler != null)
                        handler();
                }

                if (didWork)
                    this.chainStateBuilder.LogBlockchainProgress();

                lock (this.changedLock)
                {
                    if (this.changed == origChanged)
                        this.updatedEvent.Set();
                    else
                        this.NotifyWork();
                }
            }
            catch (OperationCanceledException) { }
            catch (AggregateException e)
            {
                foreach (var innerException in e.InnerExceptions)
                {
                    HandleException(innerException);
                }
            }
            catch (Exception e)
            {
                HandleException(e);
            }
        }

        private void HandleException(Exception e)
        {
            var missingException = e as MissingDataException;
            if (missingException != null)
            {
                var missingBlockHash = (UInt256)missingException.Key;
                RaiseBlockMissed(missingBlockHash);
            }
            else
            {
                this.logger.Warn("ChainStateWorker exception.", e);

                var validationException = e as ValidationException;
                if (validationException != null)
                {
                    // mark block as invalid
                    this.coreStorage.MarkBlockInvalid(validationException.BlockHash);
                }
            }
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

        private void HandleChanged(UInt256 blockHash)
        {
            HandleChanged();
        }

        private void HandleChanged(ChainedHeader chainedHeader)
        {
            HandleChanged();
        }

        private void RaiseBlockMissed(UInt256 blockHash)
        {
            if (this.lastBlockMissHash == null || this.lastBlockMissHash.Value != blockHash)
            {
                this.logger.Info("ChainStateWorker stalled, missing block: {0}".Format2(blockHash));

                this.lastBlockMissHash = blockHash;
                this.blockMissCountMeasure.Tick();
            }

            var handler = this.BlockMissed;
            if (handler != null)
                handler(blockHash);
        }
    }
}