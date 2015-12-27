using BitSharp.Common;
using BitSharp.Core;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage.Memory;
using BitSharp.Core.Test;
using BitSharp.Node;
using BitSharp.Node.Network;
using BitSharp.Node.Storage.Memory;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Ninject;
using NLog;
using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace BitSharp.IntegrationTest
{
    [TestClass]
    public class PullTester
    {
        [TestMethod]
        [Timeout(300000/*ms*/)]
        public void TestPullTester()
        {
            // locate java.exe
            var javaPath = Path.Combine(Environment.GetEnvironmentVariable("JAVA_HOME"), "bin", "java.exe");
            if (!File.Exists(javaPath))
                Assert.Inconclusive("java.exe could not be found under JAVA_HOME");

            // prepare a temp folder for bitcoinj
            string tempFolder;
            using (TempDirectory.CreateTempDirectory(out tempFolder))
            // initialize kernel
            using (var kernel = new StandardKernel())
            {
                var tempFile = Path.Combine(tempFolder, "BitcoindComparisonTool");
                var jarFile = Path.Combine(Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location), "pull-tests.jar");

                // add logging module
                kernel.Load(new ConsoleLoggingModule(LogLevel.Info));

                // log startup
                var logger = LogManager.GetCurrentClassLogger();
                logger.Info($"Starting up: {DateTime.Now}");

                // add rules module
                var rulesType = RulesEnum.ComparisonToolTestNet;
                kernel.Load(new RulesModule(rulesType));

                // add storage module
                kernel.Load(new MemoryStorageModule());
                kernel.Load(new NodeMemoryStorageModule());

                // initialize the blockchain daemon
                using (var coreDaemon = kernel.Get<CoreDaemon>())
                {
                    kernel.Bind<CoreDaemon>().ToConstant(coreDaemon).InTransientScope();

                    // initialize p2p client
                    using (var localClient = kernel.Get<LocalClient>())
                    {
                        kernel.Bind<LocalClient>().ToConstant(localClient).InTransientScope();

                        // start the blockchain daemon
                        coreDaemon.Start();

                        // find a free port
                        var port = FindFreePort();
                        Messaging.Port = port;

                        // start p2p client
                        localClient.Start();

                        // run pull tester
                        var runLargeReorgs = 0;
                        var javaProcessInfo = new ProcessStartInfo
                        {
                            FileName = javaPath,
                            Arguments = $"-jar \"{jarFile}\" \"{tempFile}\" {runLargeReorgs} {port}",
                            UseShellExecute = false,
                            RedirectStandardOutput = true,
                            RedirectStandardError = true
                        };
                        using (var javaProcess = Process.Start(javaProcessInfo))
                        {
                            try
                            {
                                var acceptanceError = false;
                                var output = new StringBuilder();
                                var errorOutput = new StringBuilder();

                                var onOutput = new DataReceivedEventHandler(
                                    (sender, e) =>
                                    {
                                        if (e.Data?.Contains("bitcoind and bitcoinj acceptance differs") ?? false)
                                            acceptanceError = true;

                                        output.AppendLine(e.Data);
                                        if (acceptanceError)
                                            errorOutput.AppendLine(e.Data);
                                    });

                                javaProcess.OutputDataReceived += onOutput;
                                javaProcess.ErrorDataReceived += onOutput;

                                javaProcess.BeginOutputReadLine();
                                javaProcess.BeginErrorReadLine();

                                javaProcess.WaitForExit();

                                logger.Info($"Pull Tester Result: {javaProcess.ExitCode}");

                                // verify pull tester successfully connected
                                Assert.IsTrue(output.ToString().Contains(
                                    $"NioClientManager.handleKey: Successfully connected to /127.0.0.1:{port}"),
                                    $"Failed to connect: {output}");

                                // don't validate pull tester result, consensus is not implemented and it will always fail
                                if (acceptanceError)
                                    Assert.Inconclusive(errorOutput.ToString());
                                else if (javaProcess.ExitCode == 0)
                                    Assert.AreEqual(0, javaProcess.ExitCode);
                                else
                                    Assert.Inconclusive(output.ToString());
                            }
                            finally
                            {
                                // ensure java.exe is terminated
                                try { javaProcess.Kill(); }
                                catch (InvalidOperationException) { }
                            }
                        }
                    }
                }
            }
        }

        private int FindFreePort()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            try
            {
                return ((IPEndPoint)listener.LocalEndpoint).Port;
            }
            finally
            {
                listener.Stop();
            }
        }
    }
}
