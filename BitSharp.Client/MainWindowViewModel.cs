using BitSharp.Common;
using BitSharp.Core;
using BitSharp.Core.Storage;
using BitSharp.Network;
using BitSharp.Wallet;
using Ninject;
using System;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Windows.Threading;

namespace BitSharp.Client
{
    public class MainWindowViewModel : IDisposable, INotifyPropertyChanged
    {
        public event PropertyChangedEventHandler PropertyChanged;

        private readonly IKernel kernel;
        private readonly CoreDaemon coreDaemon;
        private readonly CoreStorage coreStorage;
        private readonly LocalClient localClient;

        private readonly DateTimeOffset startTime;
        private string runningTime;
        private readonly DispatcherTimer runningTimeTimer;
        private readonly WorkerMethod updateWorker;

        private long _winningBlockchainHeight;
        private long _currentBlockchainHeight;
        private long _downloadedBlockCount;

        private float blockRate;
        private float transactionRate;
        private float inputRate;
        private float blockDownloadRate;
        private int duplicateBlockDownloadCount;
        private int blockMissCount;

        private readonly WalletMonitor walletMonitor;
        private int walletHeight;
        private int walletEntriesCount;
        private decimal bitBalance;
        private decimal btcBalance;

        private bool disposed;

        public MainWindowViewModel(IKernel kernel, WalletMonitor walletMonitor = null)
        {
            this.kernel = kernel;
            coreDaemon = kernel.Get<CoreDaemon>();
            coreStorage = coreDaemon.CoreStorage;
            localClient = kernel.Get<LocalClient>();
            this.walletMonitor = walletMonitor;

            startTime = DateTimeOffset.Now;
            runningTimeTimer = new DispatcherTimer();
            runningTimeTimer.Tick += (sender, e) =>
            {
                var runningTime = (DateTimeOffset.Now - startTime);
                RunningTime = $"{Math.Floor(runningTime.TotalHours):#,#00}:{runningTime:mm':'ss}";
            };
            runningTimeTimer.Interval = TimeSpan.FromMilliseconds(100);
            runningTimeTimer.Start();

            WinningBlockchainHeight = -1;
            CurrentBlockchainHeight = -1;
            DownloadedBlockCount = -1;
            WalletHeight = -1;

            updateWorker = new WorkerMethod("",
                _ =>
                {
                    WinningBlockchainHeight = coreDaemon.TargetChainHeight;
                    CurrentBlockchainHeight = coreDaemon.CurrentChain.Height;
                    DownloadedBlockCount = coreStorage.BlockWithTxesCount;

                    BlockRate = coreDaemon.GetBlockRate();
                    TransactionRate = coreDaemon.GetTxRate();
                    InputRate = coreDaemon.GetInputRate();

                    BlockDownloadRate = localClient.GetBlockDownloadRate();
                    DuplicateBlockDownloadCount = localClient.GetDuplicateBlockDownloadCount();
                    BlockMissCount = localClient.GetBlockMissCount();

                    if (walletMonitor != null)
                    {
                        WalletHeight = this.walletMonitor.WalletHeight;
                        WalletEntriesCount = this.walletMonitor.EntriesCount;
                        BitBalance = this.walletMonitor.BitBalance;
                        BtcBalance = this.walletMonitor.BtcBalance;
                    }

                    return Task.CompletedTask;
                },
                initialNotify: true, minIdleTime: TimeSpan.FromSeconds(1), maxIdleTime: TimeSpan.FromSeconds(1));
            updateWorker.Start();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposed && disposing)
            {
                updateWorker.Dispose();

                disposed = true;
            }
        }

        public string RunningTime
        {
            get { return runningTime; }
            set { SetValue(ref runningTime, value); }
        }

        public long WinningBlockchainHeight
        {
            get { return _winningBlockchainHeight; }
            set { SetValue(ref _winningBlockchainHeight, value); }
        }

        public long CurrentBlockchainHeight
        {
            get { return _currentBlockchainHeight; }
            set { SetValue(ref _currentBlockchainHeight, value); }
        }

        public long DownloadedBlockCount
        {
            get { return _downloadedBlockCount; }
            set { SetValue(ref _downloadedBlockCount, value); }
        }

        public float BlockRate
        {
            get { return blockRate; }
            set { SetValue(ref blockRate, value); }
        }

        public float TransactionRate
        {
            get { return transactionRate; }
            set { SetValue(ref transactionRate, value); }
        }

        public float InputRate
        {
            get { return inputRate; }
            set { SetValue(ref inputRate, value); }
        }

        public float BlockDownloadRate
        {
            get { return blockDownloadRate; }
            set { SetValue(ref blockDownloadRate, value); }
        }

        public int DuplicateBlockDownloadCount
        {
            get { return duplicateBlockDownloadCount; }
            set { SetValue(ref duplicateBlockDownloadCount, value); }
        }

        public int BlockMissCount
        {
            get { return blockMissCount; }
            set { SetValue(ref blockMissCount, value); }
        }

        public int WalletHeight
        {
            get { return walletHeight; }
            set { SetValue(ref walletHeight, value); }
        }

        public int WalletEntriesCount
        {
            get { return walletEntriesCount; }
            set { SetValue(ref walletEntriesCount, value); }
        }

        public decimal BitBalance
        {
            get { return bitBalance; }
            set { SetValue(ref bitBalance, value); }
        }

        public decimal BtcBalance
        {
            get { return btcBalance; }
            set { SetValue(ref btcBalance, value); }
        }

        private void SetValue<T>(ref T currentValue, T newValue, [CallerMemberName] string propertyName = "") where T : IEquatable<T>
        {
            if (currentValue == null || !currentValue.Equals(newValue))
            {
                currentValue = newValue;

                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
            }
        }
    }
}
