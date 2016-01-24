using BitSharp.Client.Helper;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core;
using BitSharp.Node;
using BitSharp.Wallet;
using BitSharp.Wallet.Address;
using NLog;
using NLog.Config;
using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Windows;

namespace BitSharp.Client
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public sealed partial class MainWindow : Window, IDisposable
    {
        private Logger logger;
        private BitSharpNode bitSharpNode;
        private MainWindowViewModel viewModel;
        private DummyMonitor dummyMonitor;

        protected override void OnInitialized(EventArgs e)
        {
            base.OnInitialized(e);

            try
            {
                // initialize logging panel
                InitUILogging(LogLevel.Info);
                logger = LogManager.GetCurrentClassLogger();

                // create node
                bitSharpNode = new BitSharpNode();

                // initialize dummy wallet monitor
                this.dummyMonitor = new DummyMonitor(bitSharpNode.CoreDaemon);

                // setup view model
                viewModel = new MainWindowViewModel(bitSharpNode.Kernel, dummyMonitor);

                DataContext = viewModel;

                // start node
                bitSharpNode.StartAsync().Forget();
            }
            catch (Exception ex)
            {
                if (logger != null)
                {
                    logger.Fatal(ex, "Application failed");
                }
                else
                {
                    MessageBox.Show($"Application failed: {ex.Message}");
                }
            }
        }

        public void Dispose()
        {
            var stopwatch = Stopwatch.StartNew();
            logger?.Info("Shutting down");

            // shutdown
            viewModel?.Dispose();
            dummyMonitor?.Dispose();
            bitSharpNode?.Dispose();

            logger?.Info($"Finished shutting down: {stopwatch.Elapsed.TotalSeconds:N2}s");
        }

        public MainWindowViewModel ViewModel => viewModel;

        protected override void OnClosing(CancelEventArgs e)
        {
            DataContext = null;
            Dispose();

            base.OnClosing(e);
        }

        private void InitUILogging(LogLevel logLevel)
        {
            // log layout format
            var layout = "${message} ${exception:separator=\r\n:format=message,type,method,stackTrace:maxInnerExceptionLevel=10:innerExceptionSeparator=\r\n:innerFormat=message,type,method,stackTrace}";

            // create rich text box target
            var uiTarget = new WpfRichTextBoxTarget
            {
                Layout = layout,
                TargetRichTextBox = loggerTextBox,
                UseDefaultRowColoringRules = true,
                AutoScroll = true,
                MaxLines = 250,
            };

            var config = LogManager.Configuration ?? new LoggingConfiguration();

            config.AddTarget("UI", uiTarget);
            config.LoggingRules.Add(new LoggingRule("*", logLevel, uiTarget.WrapAsync()));

            LogManager.Configuration = config;
        }

        private sealed class DummyMonitor : WalletMonitor
        {
            public DummyMonitor(CoreDaemon coreDaemon)
                : base(coreDaemon, keepEntries: false)
            {
                AddAddress(new First10000Address());
                AddAddress(new Top10000Address());
            }
        }
    }
}
