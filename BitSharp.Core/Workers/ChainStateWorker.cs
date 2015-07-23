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
using System.Linq;
using System.Threading.Tasks;

namespace BitSharp.Core.Workers
{
    internal class ChainStateWorker : Worker
    {
        public event Action OnChainStateChanged;

        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly IBlockchainRules rules;
        private readonly CoreStorage coreStorage;

        private readonly UpdatedTracker updatedTracker = new UpdatedTracker();

        private readonly DurationMeasure blockProcessingDurationMeasure;
        private readonly CountMeasure blockMissCountMeasure;
        private UInt256 lastBlockMissHash;

        private readonly TargetChainWorker targetChainWorker;
        private readonly ChainStateBuilder chainStateBuilder;
        private Lazy<Chain> currentChain;

        private int? maxHeight;

        public ChainStateWorker(WorkerConfig workerConfig, TargetChainWorker targetChainWorker, ChainStateBuilder chainStateBuilder, IBlockchainRules rules, CoreStorage coreStorage)
            : base("ChainStateWorker", workerConfig.initialNotify, workerConfig.minIdleTime, workerConfig.maxIdleTime)
        {
            this.rules = rules;
            this.coreStorage = coreStorage;

            this.blockProcessingDurationMeasure = new DurationMeasure(sampleCutoff: TimeSpan.FromMinutes(5));
            this.blockMissCountMeasure = new CountMeasure(TimeSpan.FromSeconds(30));

            this.targetChainWorker = targetChainWorker;
            this.chainStateBuilder = chainStateBuilder;
            this.currentChain = new Lazy<Chain>(() => this.chainStateBuilder.Chain);

            this.coreStorage.BlockInvalidated += HandleChanged;
            this.coreStorage.BlockTxesAdded += HandleChanged;
            this.coreStorage.BlockTxesRemoved += HandleChanged;
            this.coreStorage.ChainedHeaderAdded += HandleChanged;
            this.targetChainWorker.OnTargetChainChanged += HandleChanged;
        }

        public event Action<UInt256> BlockMissed;

        public int? MaxHeight
        {
            get { return maxHeight; }
            set { maxHeight = value; NotifyWork(); }
        }

        public TimeSpan AverageBlockProcessingTime()
        {
            return this.blockProcessingDurationMeasure.GetAverage();
        }

        public int GetBlockMissCount()
        {
            return this.blockMissCountMeasure.GetCount();
        }

        public Chain CurrentChain => this.currentChain.Value;

        public void WaitForUpdate()
        {
            this.updatedTracker.WaitForUpdate();
        }

        public bool WaitForUpdate(TimeSpan timeout)
        {
            return this.updatedTracker.WaitForUpdate(timeout);
        }

        protected override void SubDispose()
        {
            this.coreStorage.BlockInvalidated -= HandleChanged;
            this.coreStorage.BlockTxesAdded -= HandleChanged;
            this.coreStorage.BlockTxesRemoved -= HandleChanged;
            this.coreStorage.ChainedHeaderAdded -= HandleChanged;
            this.targetChainWorker.OnTargetChainChanged -= HandleChanged;

            this.blockProcessingDurationMeasure.Dispose();
            this.blockMissCountMeasure.Dispose();
        }

        protected override async Task WorkAction()
        {
            using (updatedTracker.TryUpdate(staleAction: NotifyWork))
            {
                try
                {
                    // calculate the new blockchain along the target path
                    var didWork = false;
                    foreach (var pathElement in this.chainStateBuilder.Chain.NavigateTowards(() => this.targetChainWorker.TargetChain)
                        .Where(pathElement => pathElement.Item2.Height <= (MaxHeight ?? int.MaxValue)))
                    {
                        // cooperative loop
                        this.ThrowIfCancelled();

                        // get block and metadata for next link in blockchain
                        var direction = pathElement.Item1;
                        var chainedHeader = pathElement.Item2;
                        IEnumerator<BlockTx> blockTxes;
                        if (!this.coreStorage.TryReadBlockTransactions(chainedHeader.Hash, /*requireTransaction:*/true, out blockTxes))
                        {
                            RaiseBlockMissed(chainedHeader.Hash);
                            break;
                        }

                        didWork = true;

                        var blockStopwatch = Stopwatch.StartNew();
                        if (direction > 0)
                        {
                            await this.chainStateBuilder.AddBlockAsync(chainedHeader, blockTxes.UsingAsEnumerable());
                        }
                        else if (direction < 0)
                        {
                            logger.Info($"Rolling back block {chainedHeader.Height:N0}: {chainedHeader.Hash}");
                            this.chainStateBuilder.RollbackBlock(chainedHeader, blockTxes.UsingAsEnumerable());
                        }
                        else
                        {
                            throw new InvalidOperationException();
                        }
                        blockStopwatch.Stop();
                        this.blockProcessingDurationMeasure.Tick(blockStopwatch.Elapsed);

                        this.currentChain = new Lazy<Chain>(() => this.chainStateBuilder.Chain);

                        this.OnChainStateChanged?.Invoke();
                    }

                    if (didWork)
                        this.chainStateBuilder.LogBlockchainProgress();
                }
                catch (OperationCanceledException) { }
                catch (AggregateException ex)
                {
                    foreach (var innerException in ex.Flatten().InnerExceptions)
                        HandleException(innerException);
                }
                catch (Exception ex)
                {
                    HandleException(ex);
                }
            }
        }

        private void HandleException(Exception ex)
        {
            var missingException = ex as MissingDataException;
            if (missingException != null)
            {
                var missingBlockHash = (UInt256)missingException.Key;
                RaiseBlockMissed(missingBlockHash);
            }
            else
            {
                logger.Warn(ex, "ChainStateWorker exception.");

                var validationException = ex as ValidationException;
                if (validationException != null)
                {
                    // mark block as invalid
                    this.coreStorage.MarkBlockInvalid(validationException.BlockHash);
                    updatedTracker.MarkStale();
                }
            }

            this.chainStateBuilder.LogBlockchainProgress();
        }

        private void HandleChanged()
        {
            updatedTracker.MarkStale();
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
            if (this.lastBlockMissHash == null || this.lastBlockMissHash != blockHash)
            {
                logger.Debug($"ChainStateWorker stalled, missing block: {blockHash}");

                this.lastBlockMissHash = blockHash;
                this.blockMissCountMeasure.Tick();
            }

            this.BlockMissed?.Invoke(blockHash);
        }
    }
}