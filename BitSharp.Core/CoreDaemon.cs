using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using BitSharp.Core.Workers;
using Ninject;
using NLog;
using System;
using System.Diagnostics;
using System.Linq;

namespace BitSharp.Core
{
    public class CoreDaemon : ICoreDaemon, IDisposable
    {
        public event EventHandler OnTargetChainChanged;
        public event EventHandler OnChainStateChanged;
        public event Action<UInt256> BlockMissed;

        private readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly IKernel kernel;
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

        public CoreDaemon(IKernel kernel, IBlockchainRules rules, IStorageManager storageManager)
        {
            this.kernel = kernel;
            this.rules = rules;
            this.storageManager = storageManager;
            this.coreStorage = new CoreStorage(storageManager);

            // write genesis block out to storage
            this.coreStorage.AddGenesisBlock(this.rules.GenesisChainedHeader);
            this.coreStorage.TryAddBlock(this.rules.GenesisBlock);

            // create chain state builder
            this.chainStateBuilder = new ChainStateBuilder(this.rules, this.storageManager);

            // add genesis block to chain state, if needed
            if (this.chainStateBuilder.Chain.Height < 0)
                this.chainStateBuilder.AddBlock(this.rules.GenesisChainedHeader, this.rules.GenesisBlock.Transactions);

            // create workers
            this.targetChainWorker = new TargetChainWorker(
                new WorkerConfig(initialNotify: true, minIdleTime: TimeSpan.FromMilliseconds(50), maxIdleTime: TimeSpan.FromSeconds(30)),
                this.rules, this.coreStorage);

            this.chainStateWorker = new ChainStateWorker(
                new WorkerConfig(initialNotify: true, minIdleTime: TimeSpan.FromMilliseconds(50), maxIdleTime: TimeSpan.FromSeconds(5)),
                this.targetChainWorker, this.chainStateBuilder, this.rules, this.coreStorage);

            this.pruningWorker = new PruningWorker(
                new WorkerConfig(initialNotify: true, minIdleTime: TimeSpan.FromSeconds(0), maxIdleTime: TimeSpan.FromMinutes(5)),
                this, this.storageManager, this.chainStateWorker);

            this.defragWorker = new DefragWorker(
                new WorkerConfig(initialNotify: true, minIdleTime: TimeSpan.FromMinutes(1), maxIdleTime: TimeSpan.FromMinutes(5)),
                this.storageManager);

            this.gcWorker = new WorkerMethod("GC Worker", GcWorker,
                initialNotify: true, minIdleTime: TimeSpan.FromSeconds(30), maxIdleTime: TimeSpan.FromMinutes(5));

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
            this.Stop();

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

        public float GetBlockRate(TimeSpan perUnitTime)
        {
            return this.chainStateBuilder.Stats.blockRateMeasure.GetAverage(perUnitTime);
        }

        public float GetTxRate(TimeSpan perUnitTime)
        {
            return this.chainStateBuilder.Stats.txRateMeasure.GetAverage(perUnitTime);
        }

        public float GetInputRate(TimeSpan perUnitTime)
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
            // startup workers
            //this.utxoScanWorker.Start();
            this.gcWorker.Start();
            this.targetChainWorker.Start();
            this.chainStateWorker.Start();
            this.pruningWorker.Start();
            this.defragWorker.Start();
        }

        public void Stop()
        {
            // stop workers
            this.defragWorker.Stop();
            this.pruningWorker.Stop();
            this.chainStateWorker.Stop();
            this.targetChainWorker.Stop();
            this.gcWorker.Stop();
            this.utxoScanWorker.Stop();
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

        private void GcWorker(WorkerMethod instance)
        {
            this.logger.Info(
                string.Join("\n",
                    new string('-', 80),
                    "GC Memory:      {0,10:#,##0.00} MB",
                    "Process Memory: {1,10:#,##0.00} MB",
                    new string('-', 80)
                )
                .Format2
                (
                /*0*/ (float)GC.GetTotalMemory(false) / 1.MILLION(),
                /*1*/ (float)Process.GetCurrentProcess().PrivateMemorySize64 / 1.MILLION()
                ));
        }

        private void UtxoScanWorker(WorkerMethod instance)
        {
            // time taking chain state snapshots
            var stopwatch = Stopwatch.StartNew();
            int chainStateHeight;
            using (var chainState = this.GetChainState())
            {
                chainStateHeight = chainState.Chain.Height;
            }
            stopwatch.Stop();
            this.logger.Info("GetChainState at {0:#,##0}: {1:#,##0.00}s".Format2(chainStateHeight, stopwatch.Elapsed.TotalSeconds));

            // time enumerating chain state snapshots
            stopwatch = Stopwatch.StartNew();
            using (var chainState = this.GetChainState())
            {
                chainStateHeight = chainState.Chain.Height;
                chainState.ReadUnspentTransactions().Count();
            }
            stopwatch.Stop();
            this.logger.Info("Enumerate chain state at {0:#,##0}: {1:#,##0.00}s".Format2(chainStateHeight, stopwatch.Elapsed.TotalSeconds));

            //using (var chainStateLocal = this.GetChainState())
            //{
            //    new MethodTimer(this.logger).Time("UTXO Commitment: {0:#,##0}".Format2(chainStateLocal.UnspentTxCount), () =>
            //    {
            //        using (var utxoStream = new UtxoStream(this.logger, chainStateLocal.ReadUnspentTransactions()))
            //        {
            //            var utxoHash = SHA256Pool.ComputeHash(utxoStream);
            //            this.logger.Info("UXO Commitment Hash: {0}".Format2(utxoHash.ToHexNumberString()));
            //        }
            //    });

            //    //new MethodTimer().Time("Full UTXO Scan: {0:#,##0}".Format2(chainStateLocal.Utxo.TransactionCount), () =>
            //    //{
            //    //    foreach (var output in chainStateLocal.Utxo.GetUnspentTransactions())
            //    //    {
            //    //    }
            //    //});
            //}
        }

        private void HandleBlockMissed(UInt256 blockHash)
        {
            var handler = this.BlockMissed;
            if (handler != null)
                handler(blockHash);
        }

        private void HandleTargetChainChanged()
        {
            var handler = this.OnTargetChainChanged;
            if (handler != null)
                handler(this, EventArgs.Empty);
        }

        private void HandleChainStateChanged()
        {
            this.pruningWorker.NotifyWork();
            this.utxoScanWorker.NotifyWork();

            var handler = this.OnChainStateChanged;
            if (handler != null)
                handler(this, EventArgs.Empty);
        }
    }
}