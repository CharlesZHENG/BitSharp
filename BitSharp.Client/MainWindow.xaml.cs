using BitSharp.Common.ExtensionMethods;
using BitSharp.Core;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage.Memory;
using BitSharp.Esent;
using BitSharp.Lmdb;
using BitSharp.Node;
using BitSharp.Node.Storage.Memory;
using BitSharp.Node.Workers;
using BitSharp.Wallet;
using BitSharp.Wallet.Address;
using Ninject;
using Ninject.Modules;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Windows;

namespace BitSharp.Client
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public sealed partial class MainWindow : Window, IDisposable
    {
        private readonly Logger logger;

        private IKernel kernel;
        private CoreDaemon coreDaemon;
        private DummyMonitor dummyMonitor;
        private LocalClient localClient;
        private MainWindowViewModel viewModel;

        public MainWindow()
        {
            try
            {
                // detect local dev machine - TODO proper configuration
                var isLocalDev = -899308969 ==
                    (Environment.MachineName + Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments)).GetHashCode();

                //TODO
                //**************************************************************
                var useTestNet = false;
                var connectToPeers = true;

                var bypassPrevTxLoading = false;
                var ignoreScripts = true;
                var ignoreSignatures = false;
                var ignoreScriptErrors = true;

                var pruningMode = PruningMode.TxIndex | PruningMode.BlockSpentIndex | PruningMode.BlockTxesPreserveMerkle;
                var enableDummyWallet = true;

                var useLmdb = false;
                var runInMemory = false;

                var cleanData = false;
                var cleanChainState = false;
                var cleanBlockTxes = false
                    // clean block txes if the chain state is being cleaned and block txes have been pruned, the new chain state will require intact blocks to validate
                    || (cleanChainState && (pruningMode.HasFlag(PruningMode.BlockTxesPreserveMerkle) || pruningMode.HasFlag(PruningMode.BlockTxesDestroyMerkle)));

                int? cacheSizeMaxBytes = 500.MILLION();

                // location to store a copy of raw blocks to avoid redownload
                if (isLocalDev)
                    BlockRequestWorker.SecondaryBlockFolder = @"D:\BitSharp.Blocks\RawBlocks";

                //NOTE: Running with a cleaned chained state against a pruned blockchain does not work.
                //      It will see the data is missing, but won't redownload the blocks.
                //**************************************************************

                // directories
                var baseDirectory = Config.LocalStoragePath;
                //if (Debugger.IsAttached)
                //    baseDirectory = Path.Combine(baseDirectory, "Debugger");

                var rulesType = useTestNet ? RulesEnum.TestNet3 : RulesEnum.MainNet;

                //TODO
                if (cleanData)
                {
                    try { Directory.Delete(Path.Combine(baseDirectory, "Data", rulesType.ToString()), recursive: true); }
                    catch (IOException) { }
                }
                if (cleanChainState)
                {
                    try { Directory.Delete(Path.Combine(baseDirectory, "Data", rulesType.ToString(), "ChainState"), recursive: true); }
                    catch (IOException) { }
                }
                if (cleanBlockTxes)
                {
                    try { Directory.Delete(Path.Combine(baseDirectory, "Data", rulesType.ToString(), "BlockTxes"), recursive: true); }
                    catch (IOException) { }
                }

                // initialize kernel
                this.kernel = new StandardKernel();

                // add logging module
                this.kernel.Load(new LoggingModule(baseDirectory, LogLevel.Info));

                // log startup
                this.logger = LogManager.GetCurrentClassLogger();
                this.logger.Info("Starting up: {0}".Format2(DateTime.Now));

                var modules = new List<INinjectModule>();

                // add storage module
                if (runInMemory)
                {
                    modules.Add(new MemoryStorageModule());
                    modules.Add(new NodeMemoryStorageModule());
                }
                else
                {
                    modules.Add(new EsentStorageModule(baseDirectory, rulesType, blockStorage: !useLmdb, cacheSizeMaxBytes: cacheSizeMaxBytes));
                    if (useLmdb)
                        modules.Add(new LmdbStorageModule(baseDirectory, rulesType));
                }

                // add rules module
                modules.Add(new RulesModule(rulesType));

                // load modules
                this.kernel.Load(modules.ToArray());

                // initialize rules
                var rules = this.kernel.Get<IBlockchainRules>();
                rules.BypassPrevTxLoading = bypassPrevTxLoading;
                rules.IgnoreScripts = ignoreScripts;
                rules.IgnoreSignatures = ignoreSignatures;
                rules.IgnoreScriptErrors = ignoreScriptErrors;

                // initialize the blockchain daemon
                this.coreDaemon = this.kernel.Get<CoreDaemon>();
                this.coreDaemon.PruningMode = pruningMode;
                this.kernel.Bind<CoreDaemon>().ToConstant(this.coreDaemon).InTransientScope();

                // initialize dummy wallet monitor
                this.dummyMonitor = new DummyMonitor(this.coreDaemon);
                if (enableDummyWallet)
                {
                    this.dummyMonitor.Start();
                }
                else
                {
                    // allow pruning to any height when not using the wallet
                    this.coreDaemon.PrunableHeight = int.MaxValue;
                }

                // initialize p2p client
                this.localClient = this.kernel.Get<LocalClient>();
                this.kernel.Bind<LocalClient>().ToConstant(this.localClient).InTransientScope();

                // setup view model
                this.viewModel = new MainWindowViewModel(this.kernel, this.dummyMonitor);
                InitializeComponent();
                this.viewModel.ViewBlockchainLast();

                // start the blockchain daemon
                this.coreDaemon.Start();

                // start p2p client
                var startThread = new Thread(() => this.localClient.Start(connectToPeers));
                startThread.Name = "LocalClient.Start";
                startThread.Start();

                this.DataContext = this.viewModel;
            }
            catch (Exception e)
            {
                if (this.logger != null)
                {
                    this.logger.Fatal("Application failed", e);
                    LogManager.Flush();
                }
                else
                {
                    Console.WriteLine(e);
                }

                Environment.Exit(-1);
            }
        }

        public void Dispose()
        {
            var stopwatch = Stopwatch.StartNew();
            this.logger.Info("Shutting down");

            // shutdown
            this.localClient.Dispose();
            this.dummyMonitor.Dispose();
            this.coreDaemon.Dispose();
            this.kernel.Dispose();

            this.logger.Info("Finished shutting down: {0:#,##0.00}s".Format2(stopwatch.Elapsed.TotalSeconds));
            LogManager.Flush();
        }

        public MainWindowViewModel ViewModel { get { return this.viewModel; } }

        protected override void OnClosing(CancelEventArgs e)
        {
            this.DataContext = null;
            Dispose();

            base.OnClosing(e);
        }

        private void ViewFirst_Click(object sender, RoutedEventArgs e)
        {
            this.viewModel.ViewBlockchainFirst();
        }

        private void ViewPrevious_Click(object sender, RoutedEventArgs e)
        {
            this.viewModel.ViewBlockchainPrevious();
        }

        private void ViewNext_Click(object sender, RoutedEventArgs e)
        {
            this.viewModel.ViewBlockchainNext();
        }

        private void ViewLast_Click(object sender, RoutedEventArgs e)
        {
            this.viewModel.ViewBlockchainLast();
        }

        private sealed class DummyMonitor : WalletMonitor
        {
            public DummyMonitor(CoreDaemon coreDaemon)
                : base(coreDaemon)
            {
                this.AddAddress(new First10000Address());
                this.AddAddress(new Top10000Address());
            }
        }
    }
}
