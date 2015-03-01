using BitSharp.Common.ExtensionMethods;
using BitSharp.Core;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage.Memory;
using BitSharp.Esent;
using BitSharp.Node;
using BitSharp.Node.Storage;
using BitSharp.Node.Storage.Memory;
using BitSharp.Node.Workers;
using BitSharp.Wallet;
using BitSharp.Wallet.Address;
using Ninject;
using Ninject.Modules;
using NLog;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Windows;

namespace BitSharp.Client
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        private IKernel kernel;
        private Logger logger;
        private CoreDaemon coreDaemon;
        private DummyMonitor dummyMonitor;
        private LocalClient localClient;
        private MainWindowViewModel viewModel;

        public MainWindow()
        {
            try
            {
                //TODO
                //**************************************************************
                var useTestNet = true;
                var connectToPeers = true;

                var bypassValidation = false;
                var ignoreScripts = false;
                var ignoreSignatures = false;
                var ignoreScriptErrors = true;

                var enablePruning = false;
                var enableDummyWallet = true;

                var runInMemory = false;

                var cleanData = false;
                var cleanChainState = false;

                var cacheSizeMaxBytes = 500.MILLION();

                // location to store a copy of raw blocks to avoid redownload
                BlockRequestWorker.SecondaryBlockFolder = @"";

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
                else if (cleanChainState)
                {
                    try { Directory.Delete(Path.Combine(baseDirectory, "Data", rulesType.ToString(), "ChainState"), recursive: true); }
                    catch (IOException) { }
                }

                // initialize kernel
                this.kernel = new StandardKernel();

                // add logging module
                this.kernel.Load(new LoggingModule(baseDirectory, LogLevel.Info));

                // log startup
                this.logger = kernel.Get<Logger>();
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
                    modules.Add(new EsentStorageModule(baseDirectory, rulesType, cacheSizeMaxBytes: cacheSizeMaxBytes));
                }

                // add cache modules
                modules.Add(new NodeCacheModule());

                // add rules module
                modules.Add(new RulesModule(rulesType));

                // load modules
                this.kernel.Load(modules.ToArray());

                // initialize rules
                var rules = this.kernel.Get<IBlockchainRules>();
                rules.BypassValidation = bypassValidation;
                rules.IgnoreScripts = ignoreScripts;
                rules.IgnoreSignatures = ignoreSignatures;
                rules.IgnoreScriptErrors = ignoreScriptErrors;

                // initialize the blockchain daemon
                this.coreDaemon = this.kernel.Get<CoreDaemon>();
                this.coreDaemon.PruningMode = enablePruning ? PruningMode.ReplayAndRollbackAndTxes : PruningMode.ReplayAndRollback;
                this.kernel.Bind<CoreDaemon>().ToConstant(this.coreDaemon).InTransientScope();

                // initialize dummy wallet monitor
                this.dummyMonitor = new DummyMonitor(this.coreDaemon, this.logger);
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

        public MainWindowViewModel ViewModel { get { return this.viewModel; } }

        protected override void OnClosed(EventArgs e)
        {
            var stopwatch = Stopwatch.StartNew();
            this.logger.Info("Shutting down");

            // shutdown
            this.localClient.Dispose();
            this.dummyMonitor.Dispose();
            this.coreDaemon.Dispose();
            this.kernel.Dispose();

            base.OnClosed(e);

            this.logger.Info("Finished shutting down: {0:#,##0.00}s".Format2(stopwatch.Elapsed.TotalSeconds));
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
            public DummyMonitor(CoreDaemon coreDaemon, Logger logger)
                : base(coreDaemon, logger)
            {
                this.AddAddress(new First10000Address());
                this.AddAddress(new Top10000Address());
            }
        }
    }
}
