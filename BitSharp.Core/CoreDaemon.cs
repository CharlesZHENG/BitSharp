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

        public event EventHandler<UnconfirmedTxAddedEventArgs> UnconfirmedTxAdded;
        public event EventHandler<TxesConfirmedEventArgs> TxesConfirmed;
        public event EventHandler<TxesUnconfirmedEventArgs> TxesUnconfirmed;

        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly ReaderWriterLockSlim controlLock = new ReaderWriterLockSlim();

        private readonly ICoreRules rules;
        private readonly IStorageManager storageManager;
        private readonly CoreStorage coreStorage;

        private readonly ChainStateBuilder chainStateBuilder;
        private readonly UnconfirmedTxesBuilder unconfirmedTxesBuilder;

        private readonly TargetChainWorker targetChainWorker;
        private readonly ChainStateWorker chainStateWorker;
        private readonly UnconfirmedTxesWorker unconfirmedTxesWorker;
        private readonly PruningWorker pruningWorker;
        private readonly DefragWorker defragWorker;
        private readonly WorkerMethod gcWorker;
        private readonly WorkerMethod utxoScanWorker;
        private readonly StatsWorker statsWorker;

        private bool isInitted;
        private bool isStarted;
        private bool isDisposed;

        public CoreDaemon(ICoreRules rules, IStorageManager storageManager)
        {
            this.rules = rules;
            this.storageManager = storageManager;
            coreStorage = new CoreStorage(storageManager);

            // create chain state builder
            chainStateBuilder = new ChainStateBuilder(this.rules, coreStorage, this.storageManager);

            // create unconfirmed txes builder
            unconfirmedTxesBuilder = new UnconfirmedTxesBuilder(this, coreStorage, this.storageManager);

            // create workers
            targetChainWorker = new TargetChainWorker(
                new WorkerConfig(initialNotify: true, minIdleTime: TimeSpan.FromMilliseconds(0), maxIdleTime: TimeSpan.FromSeconds(30)),
                ChainParams, coreStorage);

            chainStateWorker = new ChainStateWorker(
                new WorkerConfig(initialNotify: true, minIdleTime: TimeSpan.FromMilliseconds(0), maxIdleTime: TimeSpan.FromSeconds(5)),
                targetChainWorker, chainStateBuilder, this.rules, coreStorage);

            unconfirmedTxesWorker = new UnconfirmedTxesWorker(
                new WorkerConfig(initialNotify: true, minIdleTime: TimeSpan.FromMilliseconds(0), maxIdleTime: TimeSpan.FromSeconds(5)),
                chainStateWorker, unconfirmedTxesBuilder, coreStorage);

            pruningWorker = new PruningWorker(
                new WorkerConfig(initialNotify: true, minIdleTime: TimeSpan.FromSeconds(0), maxIdleTime: TimeSpan.FromMinutes(5)),
                this, this.storageManager, chainStateWorker);

            defragWorker = new DefragWorker(
                new WorkerConfig(initialNotify: true, minIdleTime: TimeSpan.FromMinutes(5), maxIdleTime: TimeSpan.FromMinutes(5)),
                this.storageManager);

            gcWorker = new WorkerMethod("GC Worker", GcWorker,
                initialNotify: true, minIdleTime: TimeSpan.FromMinutes(5), maxIdleTime: TimeSpan.FromMinutes(5));

            utxoScanWorker = new WorkerMethod("UTXO Scan Worker", UtxoScanWorker,
                initialNotify: true, minIdleTime: TimeSpan.FromSeconds(60), maxIdleTime: TimeSpan.FromSeconds(60));

            statsWorker = new StatsWorker(
                new WorkerConfig(initialNotify: true, minIdleTime: TimeSpan.FromMinutes(0), maxIdleTime: TimeSpan.MaxValue),
                this);

            // wire events
            chainStateWorker.BlockMissed += HandleBlockMissed;
            targetChainWorker.OnTargetChainChanged += HandleTargetChainChanged;
            chainStateWorker.OnChainStateChanged += HandleChainStateChanged;
            pruningWorker.OnWorkFinished += defragWorker.NotifyWork;
            unconfirmedTxesBuilder.UnconfirmedTxAdded += RaiseUnconfirmedTxAdded;
            unconfirmedTxesBuilder.TxesConfirmed += RaiseTxesConfirmed;
            unconfirmedTxesBuilder.TxesUnconfirmed += RaiseTxesUnconfirmed;
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
                chainStateWorker.BlockMissed -= HandleBlockMissed;
                targetChainWorker.OnTargetChainChanged -= HandleTargetChainChanged;
                chainStateWorker.OnChainStateChanged -= HandleChainStateChanged;
                pruningWorker.OnWorkFinished -= defragWorker.NotifyWork;
                unconfirmedTxesBuilder.UnconfirmedTxAdded -= RaiseUnconfirmedTxAdded;
                unconfirmedTxesBuilder.TxesConfirmed -= RaiseTxesConfirmed;
                unconfirmedTxesBuilder.TxesUnconfirmed -= RaiseTxesUnconfirmed;

                // cleanup workers
                statsWorker.Dispose();
                defragWorker.Dispose();
                pruningWorker.Dispose();
                unconfirmedTxesWorker.Dispose();
                chainStateWorker.Dispose();
                targetChainWorker.Dispose();
                gcWorker.Dispose();
                utxoScanWorker.Dispose();
                unconfirmedTxesBuilder.Dispose();
                chainStateBuilder.Dispose();
                coreStorage.Dispose();
                controlLock.Dispose();

                isDisposed = true;
            }
        }

        public CoreStorage CoreStorage => coreStorage;

        public IChainParams ChainParams => rules.ChainParams;

        public Chain TargetChain => targetChainWorker.TargetChain;

        public int TargetChainHeight => targetChainWorker.TargetChain?.Height ?? -1;

        public Chain CurrentChain => chainStateWorker.CurrentChain;

        public Chain UnconfirmedTxesChain => unconfirmedTxesWorker.CurrentChain;

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
            get { return pruningWorker.Mode; }
            set { pruningWorker.Mode = value; }
        }

        //TODO any replayers should register their chain tip with CoreDaemon, and update it as the replay
        //TODO CoreDaemon can then keep track of all the chains and determine how much can safely be pruned
        //TODO the pruning of rollback replay information would also be coordinated against the registered chain tips
        public int PrunableHeight
        {
            get { return pruningWorker.PrunableHeight; }
            set { pruningWorker.PrunableHeight = value; }
        }

        public float GetBlockRate(TimeSpan? perUnitTime = null)
        {
            return chainStateBuilder.Stats.blockRateMeasure.GetAverage(perUnitTime);
        }

        public float GetTxRate(TimeSpan? perUnitTime = null)
        {
            return chainStateBuilder.Stats.txRateMeasure.GetAverage(perUnitTime);
        }

        public float GetInputRate(TimeSpan? perUnitTime = null)
        {
            return chainStateBuilder.Stats.inputRateMeasure.GetAverage(perUnitTime);
        }

        public TimeSpan AverageBlockProcessingTime()
        {
            return chainStateWorker.AverageBlockProcessingTime();
        }

        public int GetBlockMissCount()
        {
            return chainStateWorker.GetBlockMissCount();
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
                coreStorage.AddGenesisBlock(ChainParams.GenesisChainedHeader);
                coreStorage.TryAddBlock(ChainParams.GenesisBlock);

                isInitted = true;
            }

            // startup workers
            //this.utxoScanWorker.Start();
            gcWorker.Start();
            targetChainWorker.Start();
            chainStateWorker.Start();
            unconfirmedTxesWorker.Start();
            pruningWorker.Start();
            defragWorker.Start();
            statsWorker.Start();
            isStarted = true;
        }

        public void Stop()
        {
            controlLock.DoWrite(() => InternalStop());
        }

        private void InternalStop()
        {
            // stop workers
            statsWorker.Stop();
            defragWorker.Stop();
            pruningWorker.Stop();
            unconfirmedTxesWorker.Stop();
            chainStateWorker.Stop();
            targetChainWorker.Stop();
            gcWorker.Stop();
            utxoScanWorker.Stop();
            isStarted = false;
        }

        public void WaitForUpdate()
        {
            targetChainWorker.WaitForUpdate();
            chainStateWorker.WaitForUpdate();
        }

        public void ForceUpdate()
        {
            targetChainWorker.ForceUpdate();
            chainStateWorker.ForceUpdate();
        }

        public void ForceUpdateAndWait()
        {
            targetChainWorker.ForceUpdateAndWait();
            chainStateWorker.ForceUpdateAndWait();
        }

        //TODO need to implement functionality to prevent pruning from removing block data that is being used by chain state snapshots
        //TODO i.e. don't prune past height X
        public IChainState GetChainState()
        {
            return chainStateBuilder.ToImmutable();
        }

        public IUnconfirmedTxes GetUnconfirmedTxes() => unconfirmedTxesBuilder.ToImmutable();

        public bool TryAddUnconfirmedTx(DecodedTx decodedTx)
        {
            return unconfirmedTxesBuilder.TryAddTransaction(decodedTx);
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

            return Task.CompletedTask;
        }

        private Task UtxoScanWorker(WorkerMethod instance)
        {
            // time taking chain state snapshots
            var stopwatch = Stopwatch.StartNew();
            int chainStateHeight;
            using (var chainState = GetChainState())
            {
                chainStateHeight = chainState.Chain.Height;
            }
            stopwatch.Stop();
            logger.Info($"GetChainState at {chainStateHeight:N0}: {stopwatch.Elapsed.TotalSeconds:N2}s");

            // time enumerating chain state snapshots
            stopwatch = Stopwatch.StartNew();
            using (var chainState = GetChainState())
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

            return Task.CompletedTask;
        }

        private void HandleBlockMissed(UInt256 blockHash)
        {
            BlockMissed?.Invoke(blockHash);
        }

        private void HandleTargetChainChanged()
        {
            OnTargetChainChanged?.Invoke(this, EventArgs.Empty);
        }

        private void HandleChainStateChanged()
        {
            pruningWorker.NotifyWork();
            utxoScanWorker.NotifyWork();

            OnChainStateChanged?.Invoke(this, EventArgs.Empty);
        }

        private void RaiseUnconfirmedTxAdded(object sender, UnconfirmedTxAddedEventArgs e)
        {
            UnconfirmedTxAdded?.Invoke(this, e);
        }

        private void RaiseTxesConfirmed(object sender, TxesConfirmedEventArgs e)
        {
            TxesConfirmed?.Invoke(this, e);
        }

        private void RaiseTxesUnconfirmed(object sender, TxesUnconfirmedEventArgs e)
        {
            TxesUnconfirmed?.Invoke(this, e);
        }
    }
}