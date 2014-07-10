﻿using BitSharp.Common.ExtensionMethods;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.ComponentModel;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Input;
using System.Windows.Threading;
using Ninject;
using BitSharp.Core;
using BitSharp.Core.Storage;
using BitSharp.Core.Domain;
using BitSharp.Node;
using System.Collections.ObjectModel;
using BitSharp.Wallet;

namespace BitSharp.Client
{
    public class MainWindowViewModel : INotifyPropertyChanged
    {
        public event PropertyChangedEventHandler PropertyChanged;

        private readonly IKernel kernel;
        private readonly CoreDaemon coreDaemon;
        private readonly CoreStorage coreStorage;
        private readonly LocalClient localClient;

        private readonly DateTime startTime;
        private string runningTime;
        private readonly DispatcherTimer runningTimeTimer;
        private readonly DispatcherTimer ratesTimer;

        private Chain viewChain;

        private long _winningBlockchainHeight;
        private long _currentBlockchainHeight;
        private long _downloadedBlockCount;

        private float blockRate;
        private float transactionRate;
        private float inputRate;
        private float blockDownloadRate;
        private float duplicateBlockDownloadRate;
        private float blockMissRate;

        private readonly WalletMonitor walletMonitor;
        private decimal bitBalance;
        private readonly Dispatcher dispatcher;

        public MainWindowViewModel(IKernel kernel, WalletMonitor walletMonitor = null)
        {
            this.dispatcher = Dispatcher.CurrentDispatcher;

            this.kernel = kernel;
            this.coreDaemon = kernel.Get<CoreDaemon>();
            this.coreStorage = this.coreDaemon.CoreStorage;
            this.localClient = kernel.Get<LocalClient>();

            this.startTime = DateTime.UtcNow;
            this.runningTimeTimer = new DispatcherTimer();
            runningTimeTimer.Tick += (sender, e) =>
            {
                var runningTime = (DateTime.UtcNow - this.startTime);
                this.RunningTime = "{0:#,#00}:{1:mm':'ss}".Format2(Math.Floor(runningTime.TotalHours), runningTime);
            };
            runningTimeTimer.Interval = TimeSpan.FromMilliseconds(100);
            runningTimeTimer.Start();

            this.ratesTimer = new DispatcherTimer();
            ratesTimer.Tick += (sender, e) =>
            {
                this.BlockRate = this.coreDaemon.GetBlockRate(TimeSpan.FromSeconds(1));
                this.TransactionRate = this.coreDaemon.GetTxRate(TimeSpan.FromSeconds(1));
                this.InputRate = this.coreDaemon.GetInputRate(TimeSpan.FromSeconds(1));
                this.BlockDownloadRate = this.localClient.GetBlockDownloadRate(TimeSpan.FromSeconds(1));
                this.DuplicateBlockDownloadRate = this.localClient.GetDuplicateBlockDownloadRate(TimeSpan.FromSeconds(1));
                this.BlockMissRate = this.localClient.GetBlockMissRate(TimeSpan.FromSeconds(1));
            };
            ratesTimer.Interval = TimeSpan.FromSeconds(1);
            ratesTimer.Start();

            this.viewChain = this.coreDaemon.CurrentChain;

            this.WinningBlockchainHeight = this.coreDaemon.TargetBlockHeight;
            this.CurrentBlockchainHeight = this.coreDaemon.CurrentChain.Height;
            this.DownloadedBlockCount = this.coreStorage.BlockWithTxesCount;

            this.coreStorage.BlockTxesAdded +=
                (chainedHeader) =>
                    DownloadedBlockCount = this.coreStorage.BlockWithTxesCount;

            this.coreStorage.BlockTxesRemoved +=
                (blockHash) =>
                    DownloadedBlockCount = this.coreStorage.BlockWithTxesCount;

            this.coreDaemon.OnTargetBlockChanged +=
                (sender, block) =>
                    WinningBlockchainHeight = this.coreDaemon.TargetBlockHeight;

            this.coreDaemon.OnChainStateChanged +=
                (sender, chainState) =>
                    CurrentBlockchainHeight = this.coreDaemon.CurrentChain.Height;

            if (walletMonitor != null)
            {
                this.walletMonitor = walletMonitor;
                this.WalletEntries = new ObservableCollection<WalletEntry>();
                this.walletMonitor.OnEntryAdded += HandleOnWalletEntryAdded;
            }
        }

        public string RunningTime
        {
            get { return this.runningTime; }
            set { SetValue(ref this.runningTime, value); }
        }

        public long WinningBlockchainHeight
        {
            get { return this._winningBlockchainHeight; }
            set { SetValue(ref this._winningBlockchainHeight, value); }
        }

        public long CurrentBlockchainHeight
        {
            get { return this._currentBlockchainHeight; }
            set { SetValue(ref this._currentBlockchainHeight, value); }
        }

        public long DownloadedBlockCount
        {
            get { return this._downloadedBlockCount; }
            set { SetValue(ref this._downloadedBlockCount, value); }
        }

        public float BlockRate
        {
            get { return this.blockRate; }
            set { SetValue(ref this.blockRate, value); }
        }

        public float TransactionRate
        {
            get { return this.transactionRate; }
            set { SetValue(ref this.transactionRate, value); }
        }

        public float InputRate
        {
            get { return this.inputRate; }
            set { SetValue(ref this.inputRate, value); }
        }

        public float BlockDownloadRate
        {
            get { return this.blockDownloadRate; }
            set { SetValue(ref this.blockDownloadRate, value); }
        }

        public float DuplicateBlockDownloadRate
        {
            get { return this.duplicateBlockDownloadRate; }
            set { SetValue(ref this.duplicateBlockDownloadRate, value); }
        }

        public float BlockMissRate
        {
            get { return this.blockMissRate; }
            set { SetValue(ref this.blockMissRate, value); }
        }

        public long ViewBlockchainHeight
        {
            get
            {
                return this.viewChain.Height;
            }
            set
            {
                var chainLocal = this.coreDaemon.CurrentChain;

                var height = value;
                if (height > chainLocal.Height)
                    height = chainLocal.Height;

                var targetBlock = chainLocal.LastBlock;
                SetViewBlockchain(targetBlock);
            }
        }

        public IList<TxOutputKey> ViewBlockchainSpendOutputs { get; protected set; }

        public IList<TxOutputKey> ViewBlockchainReceiveOutputs { get; protected set; }

        public ObservableCollection<WalletEntry> WalletEntries { get; protected set; }

        public decimal BitBalance
        {
            get { return this.bitBalance; }
            set { SetValue(ref this.bitBalance, value); }
        }

        public void ViewBlockchainFirst()
        {
            var chainLocal = this.coreDaemon.CurrentChain;
            if (chainLocal.Blocks.Count == 0)
                return;

            var targetBlock = chainLocal.Blocks.First();
            SetViewBlockchain(targetBlock);
        }

        public void ViewBlockchainPrevious()
        {
            var chainLocal = this.coreDaemon.CurrentChain;
            if (chainLocal.Blocks.Count == 0)
                return;

            var height = this.viewChain.Height - 1;
            if (height > chainLocal.Height)
                height = chainLocal.Height;

            var targetBlock = chainLocal.LastBlock;
            SetViewBlockchain(targetBlock);
        }

        public void ViewBlockchainNext()
        {
            var chainLocal = this.coreDaemon.CurrentChain;
            if (chainLocal.Blocks.Count == 0)
                return;

            var height = this.viewChain.Height + 1;
            if (height > chainLocal.Height)
                height = chainLocal.Height;

            var targetBlock = chainLocal.LastBlock;
            SetViewBlockchain(targetBlock);
        }

        public void ViewBlockchainLast()
        {
            SetViewBlockchain(this.coreDaemon.CurrentChain);
        }

        private void SetViewBlockchain(ChainedHeader targetBlock)
        {
            using (var cancelToken = new CancellationTokenSource())
            {
                try
                {
                    //TODO
                    //List<MissingDataException> missingData;
                    //var blockchain = this.blockchainDaemon.Calculator.CalculateBlockchainFromExisting(this.viewBlockchain, targetBlock, null, out missingData, cancelToken.Token);
                    //SetViewBlockchain(blockchain);
                }
                catch (MissingDataException)
                {
                    // TODO
                }
                catch (AggregateException e)
                {
                    if (!e.IsMissingDataOnly())
                        throw;
                }
            }
        }

        private void SetViewBlockchain(Chain chain)
        {
            this.viewChain = chain;

            try
            {
                if (chain.Height > 0)
                {
                    //Block block;
                    //this.blockCache.TryGetValue(this.viewChain.LastBlockHash, out block);
                    // TODO this is abusing rollback a bit just to get the transactions that exist in a target block that's already known
                    // TODO make a better api for get the net output of a block
                    //List<TxOutputKey> spendOutputs, receiveOutputs;
                    //TODO
                    //this.blockchainDaemon.Calculator.RollbackBlockchain(this.viewBlockchain, block, out spendOutputs, out receiveOutputs);

                    //TODO
                    //ViewBlockchainSpendOutputs = spendOutputs;
                    //ViewBlockchainReceiveOutputs = receiveOutputs;
                }
                else
                {
                    ViewBlockchainSpendOutputs = new List<TxOutputKey>();
                    ViewBlockchainReceiveOutputs = new List<TxOutputKey>();
                }
            }
            catch (MissingDataException)
            {
                // TODO
            }
            catch (AggregateException e)
            {
                if (!e.IsMissingDataOnly())
                    throw;
            }

            var handler = this.PropertyChanged;
            if (handler != null)
            {
                handler(this, new PropertyChangedEventArgs("ViewBlockchainHeight"));
                handler(this, new PropertyChangedEventArgs("ViewBlockchainSpendOutputs"));
                handler(this, new PropertyChangedEventArgs("ViewBlockchainReceiveOutputs"));
            }
        }

        private void HandleOnWalletEntryAdded(WalletEntry walletEntry)
        {
            this.dispatcher.BeginInvoke((Action)(() =>
                this.WalletEntries.Insert(0, walletEntry)));

            this.BitBalance = this.walletMonitor.BitBalance;
        }

        private void SetValue<T>(ref T currentValue, T newValue, [CallerMemberName] string propertyName = "") where T : IEquatable<T>
        {
            if (currentValue == null || !currentValue.Equals(newValue))
            {
                currentValue = newValue;

                var handler = this.PropertyChanged;
                if (handler != null)
                    handler(this, new PropertyChangedEventArgs(propertyName));
            }
        }
    }
}
