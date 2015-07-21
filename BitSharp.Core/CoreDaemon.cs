using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using BitSharp.Core.Workers;
using NLog;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Core
{
    public class CoreDaemon : ICoreDaemon, IDisposable
    {
        public event EventHandler OnTargetChainChanged;
        public event EventHandler OnChainStateChanged;
        public event Action<UInt256> BlockMissed;

        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly ReaderWriterLockSlim controlLock = new ReaderWriterLockSlim();

        private readonly IBlockchainRules rules;
        private readonly IStorageManager storageManager;
        private readonly CoreStorage coreStorage;

        private readonly ChainStateBuilder chainStateBuilder;

        private readonly TargetChainWorker targetChainWorker;
        private readonly ChainStateWorker chainStateWorker;
        private readonly PruningWorker pruningWorker;
        private readonly DefragWorker defragWorker;
        private readonly WorkerMethod gcWorker;
        private readonly WorkerMethod utxoScanWorker;

        private bool isInitted;
        private bool isStarted;
        private bool isDisposed;

        public CoreDaemon(IBlockchainRules rules, IStorageManager storageManager)
        {
            this.rules = rules;
            this.storageManager = storageManager;
            this.coreStorage = new CoreStorage(storageManager);

            // create chain state builder
            this.chainStateBuilder = new ChainStateBuilder(this.rules, this.coreStorage, this.storageManager);

            // create workers
            this.targetChainWorker = new TargetChainWorker(
                new WorkerConfig(initialNotify: true, minIdleTime: TimeSpan.FromMilliseconds(50), maxIdleTime: TimeSpan.FromSeconds(30)),
                this.rules, this.coreStorage);

            this.chainStateWorker = new ChainStateWorker(
                new WorkerConfig(initialNotify: true, minIdleTime: TimeSpan.FromMilliseconds(0), maxIdleTime: TimeSpan.FromSeconds(5)),
                this.targetChainWorker, this.chainStateBuilder, this.rules, this.coreStorage);

            this.pruningWorker = new PruningWorker(
                new WorkerConfig(initialNotify: true, minIdleTime: TimeSpan.FromSeconds(0), maxIdleTime: TimeSpan.FromMinutes(5)),
                this, this.storageManager, this.chainStateWorker);

            this.defragWorker = new DefragWorker(
                new WorkerConfig(initialNotify: true, minIdleTime: TimeSpan.FromMinutes(5), maxIdleTime: TimeSpan.FromMinutes(5)),
                this.storageManager);

            this.gcWorker = new WorkerMethod("GC Worker", GcWorker,
                initialNotify: true, minIdleTime: TimeSpan.FromMinutes(5), maxIdleTime: TimeSpan.FromMinutes(5));

            this.utxoScanWorker = new WorkerMethod("UTXO Scan Worker", UtxoScanWorker,
                initialNotify: true, minIdleTime: TimeSpan.FromSeconds(60), maxIdleTime: TimeSpan.FromSeconds(60));

            // wire events
            this.chainStateWorker.BlockMissed += HandleBlockMissed;
            this.targetChainWorker.OnTargetChainChanged += HandleTargetChainChanged;
            this.chainStateWorker.OnChainStateChanged += HandleChainStateChanged;
            this.pruningWorker.OnWorkFinished += this.defragWorker.NotifyWork;
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
                // unwire events
                this.chainStateWorker.BlockMissed -= HandleBlockMissed;
                this.targetChainWorker.OnTargetChainChanged -= HandleTargetChainChanged;
                this.chainStateWorker.OnChainStateChanged -= HandleChainStateChanged;
                this.pruningWorker.OnWorkFinished -= this.defragWorker.NotifyWork;

                // cleanup workers
                this.defragWorker.Dispose();
                this.pruningWorker.Dispose();
                this.chainStateWorker.Dispose();
                this.targetChainWorker.Dispose();
                this.gcWorker.Dispose();
                this.utxoScanWorker.Dispose();
                this.chainStateBuilder.Dispose();
                this.coreStorage.Dispose();
                this.controlLock.Dispose();

                isDisposed = true;
            }
        }

        public CoreStorage CoreStorage { get { return this.coreStorage; } }

        public IBlockchainRules Rules { get { return this.rules; } }

        public Chain TargetChain { get { return this.targetChainWorker.TargetChain; } }

        public int TargetChainHeight
        {
            get
            {
                var targetChainLocal = this.targetChainWorker.TargetChain;
                if (targetChainLocal != null)
                    return targetChainLocal.Height;
                else
                    return -1;
            }
        }

        public Chain CurrentChain
        {
            get { return this.chainStateWorker.CurrentChain; }
        }

        public int? MaxHeight
        {
            get { return chainStateWorker.MaxHeight; }
            set { chainStateWorker.MaxHeight = value; }
        }

        public bool IsStarted
        {
            get { return controlLock.DoRead(() => isStarted); }
            set
            {
                controlLock.DoWrite(() =>
                {
                    if (isStarted == value)
                        return;

                    if (value)
                        InternalStart();
                    else
                        InternalStop();
                });
            }
        }

        public PruningMode PruningMode
        {
            get { return this.pruningWorker.Mode; }
            set { this.pruningWorker.Mode = value; }
        }

        //TODO any replayers should register their chain tip with CoreDaemon, and update it as the replay
        //TODO CoreDaemon can then keep track of all the chains and determine how much can safely be pruned
        //TODO the pruning of rollback replay information would also be coordinated against the registered chain tips
        public int PrunableHeight
        {
            get { return this.pruningWorker.PrunableHeight; }
            set { this.pruningWorker.PrunableHeight = value; }
        }

        public float GetBlockRate(TimeSpan? perUnitTime = null)
        {
            return this.chainStateBuilder.Stats.blockRateMeasure.GetAverage(perUnitTime);
        }

        public float GetTxRate(TimeSpan? perUnitTime = null)
        {
            return this.chainStateBuilder.Stats.txRateMeasure.GetAverage(perUnitTime);
        }

        public float GetInputRate(TimeSpan? perUnitTime = null)
        {
            return this.chainStateBuilder.Stats.inputRateMeasure.GetAverage(perUnitTime);
        }

        public TimeSpan AverageBlockProcessingTime()
        {
            return this.chainStateWorker.AverageBlockProcessingTime();
        }

        public int GetBlockMissCount()
        {
            return this.chainStateWorker.GetBlockMissCount();
        }

        public void Start()
        {
            controlLock.DoWrite(() => InternalStart());
        }

        private void InternalStart()
        {
            if (!isInitted)
            {
                // write genesis block out to storage
                this.coreStorage.AddGenesisBlock(this.rules.GenesisChainedHeader);
                this.coreStorage.TryAddBlock(this.rules.GenesisBlock);

                isInitted = true;
            }

            // startup workers
            //this.utxoScanWorker.Start();
            this.gcWorker.Start();
            this.targetChainWorker.Start();
            this.chainStateWorker.Start();
            this.pruningWorker.Start();
            this.defragWorker.Start();
            isStarted = true;
        }

        public void Stop()
        {
            controlLock.DoWrite(() => InternalStop());
        }

        private void InternalStop()
        {
            // stop workers
            this.defragWorker.Stop();
            this.pruningWorker.Stop();
            this.chainStateWorker.Stop();
            this.targetChainWorker.Stop();
            this.gcWorker.Stop();
            this.utxoScanWorker.Stop();
            isStarted = false;
        }

        public void WaitForUpdate()
        {
            this.targetChainWorker.WaitForUpdate();
            this.chainStateWorker.WaitForUpdate();
        }

        //TODO need to implement functionality to prevent pruning from removing block data that is being used by chain state snapshots
        //TODO i.e. don't prune past height X
        public IChainState GetChainState()
        {
            return this.chainStateBuilder.ToImmutable();
        }

        private Task GcWorker(WorkerMethod instance)
        {
            logger.Info(
                string.Join("\n",
                    new string('-', 80),
                    $"GC Memory:      {(float)GC.GetTotalMemory(false) / 1.MILLION(),10:N2} MB",
                    $"Process Memory: {(float)Process.GetCurrentProcess().PrivateMemorySize64 / 1.MILLION(),10:N2} MB",
                    new string('-', 80)
                ));

            return Task.FromResult(false);
        }

        private Task UtxoScanWorker(WorkerMethod instance)
        {
            // time taking chain state snapshots
            var stopwatch = Stopwatch.StartNew();
            int chainStateHeight;
            using (var chainState = this.GetChainState())
            {
                chainStateHeight = chainState.Chain.Height;
            }
            stopwatch.Stop();
            logger.Info($"GetChainState at {chainStateHeight:N0}: {stopwatch.Elapsed.TotalSeconds:N2}s");

            // time enumerating chain state snapshots
            stopwatch = Stopwatch.StartNew();
            using (var chainState = this.GetChainState())
            {
                chainStateHeight = chainState.Chain.Height;
                chainState.ReadUnspentTransactions().Count();
            }
            stopwatch.Stop();
            logger.Info($"Enumerate chain state at {chainStateHeight:N0}: {stopwatch.Elapsed.TotalSeconds:N2}s");

            //using (var chainStateLocal = this.GetChainState())
            //{
            //    new MethodTimer(logger).Time("UTXO Commitment: {0:N0}".Format2(chainStateLocal.UnspentTxCount), () =>
            //    {
            //        using (var utxoStream = new UtxoStream(logger, chainStateLocal.ReadUnspentTransactions()))
            //        {
            //            var utxoHash = SHA256Pool.ComputeHash(utxoStream);
            //            logger.Info("UXO Commitment Hash: {0}".Format2(utxoHash.ToHexNumberString()));
            //        }
            //    });

            //    //new MethodTimer().Time("Full UTXO Scan: {0:N0}".Format2(chainStateLocal.Utxo.TransactionCount), () =>
            //    //{
            //    //    foreach (var output in chainStateLocal.Utxo.GetUnspentTransactions())
            //    //    {
            //    //    }
            //    //});
            //}

            return Task.FromResult(false);
        }

        private void HandleBlockMissed(UInt256 blockHash)
        {
            this.BlockMissed?.Invoke(blockHash);
        }

        private void HandleTargetChainChanged()
        {
            this.OnTargetChainChanged?.Invoke(this, EventArgs.Empty);
        }

        private void HandleChainStateChanged()
        {
            this.pruningWorker.NotifyWork();
            this.utxoScanWorker.NotifyWork();

            this.OnChainStateChanged?.Invoke(this, EventArgs.Empty);
        }
    }
}