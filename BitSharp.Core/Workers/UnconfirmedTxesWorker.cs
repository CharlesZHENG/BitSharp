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
    internal class UnconfirmedTxesWorker : Worker
    {
        public event EventHandler OnChanged;
        public event EventHandler<UInt256> BlockMissed;

        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly CoreStorage coreStorage;

        private readonly UpdatedTracker updatedTracker = new UpdatedTracker();

        private readonly ChainStateWorker chainStateWorker;
        private readonly UnconfirmedTxesBuilder unconfirmedTxesBuilder;
        private Lazy<Chain> currentChain;

        public UnconfirmedTxesWorker(WorkerConfig workerConfig, ChainStateWorker chainStateWorker, UnconfirmedTxesBuilder unconfirmedTxesBuilder, CoreStorage coreStorage)
            : base("UnconfirmedTxesWorker", workerConfig.initialNotify, workerConfig.minIdleTime, workerConfig.maxIdleTime)
        {
            this.coreStorage = coreStorage;

            this.chainStateWorker = chainStateWorker;
            this.unconfirmedTxesBuilder = unconfirmedTxesBuilder;

            this.currentChain = new Lazy<Chain>(() => this.unconfirmedTxesBuilder.Chain);

            this.chainStateWorker.OnChainStateChanged += HandleChanged;
        }

        public Chain CurrentChain => this.currentChain.Value;

        public void WaitForUpdate()
        {
            updatedTracker.WaitForUpdate();
        }

        public bool WaitForUpdate(TimeSpan timeout)
        {
            return updatedTracker.WaitForUpdate(timeout);
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

        protected override void SubDispose()
        {
            chainStateWorker.OnChainStateChanged -= HandleChanged;
        }

        protected override Task WorkAction()
        {
            using (updatedTracker.TryUpdate(staleAction: NotifyWork))
            {
                try
                {
                    foreach (var pathElement in unconfirmedTxesBuilder.Chain.NavigateTowards(() => chainStateWorker.CurrentChain))
                    {
                        // cooperative loop
                        ThrowIfCancelled();

                        // get block and metadata for next link in blockchain
                        var direction = pathElement.Item1;
                        var chainedHeader = pathElement.Item2;
                        IEnumerator<BlockTx> blockTxes;
                        if (!coreStorage.TryReadBlockTransactions(chainedHeader.Hash, out blockTxes))
                        {
                            BlockMissed?.Invoke(this, chainedHeader.Hash);
                            break;
                        }

                        if (direction > 0)
                            unconfirmedTxesBuilder.AddBlock(chainedHeader, blockTxes.UsingAsEnumerable());
                        else if (direction < 0)
                            unconfirmedTxesBuilder.RollbackBlock(chainedHeader, blockTxes.UsingAsEnumerable());
                        else
                            throw new InvalidOperationException();

                        currentChain = new Lazy<Chain>(() => unconfirmedTxesBuilder.Chain);

                        OnChanged?.Invoke(this, EventArgs.Empty);
                    }
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

            return Task.CompletedTask;
        }

        private void HandleException(Exception ex)
        {
            var missingException = ex as MissingDataException;
            if (missingException != null)
            {
                var missingBlockHash = (UInt256)missingException.Key;
                BlockMissed?.Invoke(this, missingBlockHash);
            }
            else
            {
                logger.Warn(ex, "UnconfirmedTxesWorker exception.");
            }
        }

        private void HandleChanged()
        {
            updatedTracker.MarkStale();
            NotifyWork();
        }
    }
}