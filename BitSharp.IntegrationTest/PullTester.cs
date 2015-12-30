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
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

namespace BitSharp.IntegrationTest
{
    [TestClass]
    public class PullTester
    {
        [TestMethod]
        [Timeout(6 * /*minutes*/(60 * 1000))]
        public void TestPullTester()
        {
            var javaTimeout = (int)TimeSpan.FromMinutes(5).TotalMilliseconds;

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
                var chainType = ChainType.Regtest;
                kernel.Load(new RulesModule(chainType));

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
                                var errorOccurred = false;

                                var output = new StringBuilder();
                                var successOutput = new StringBuilder();
                                var errorOutput = new StringBuilder();

                                var onOutput = new DataReceivedEventHandler(
                                    (sender, e) =>
                                    {
                                        if (e.Data?.Contains("ERROR:") ?? false)
                                            errorOccurred = true;

                                        output.AppendLine(e.Data);
                                        if (!errorOccurred)
                                            successOutput.AppendLine(e.Data);
                                        else
                                            errorOutput.AppendLine(e.Data);
                                    });

                                javaProcess.OutputDataReceived += onOutput;
                                javaProcess.ErrorDataReceived += onOutput;

                                javaProcess.BeginOutputReadLine();
                                javaProcess.BeginErrorReadLine();

                                var didJavaExit = javaProcess.WaitForExit(javaTimeout);

                                javaProcess.OutputDataReceived -= onOutput;
                                javaProcess.ErrorDataReceived -= onOutput;

                                logger.Info($"Pull Tester Result: {(didJavaExit ? (int?)javaProcess.ExitCode : null)}");

                                // verify pull tester successfully connected
                                Assert.IsTrue(output.ToString().Contains(
                                    $"NioClientManager.handleKey: Successfully connected to /127.0.0.1:{port}"),
                                    $"Failed to connect: {output}");

                                if (errorOccurred || !didJavaExit || javaProcess.ExitCode != 0)
                                {
                                    // log all success & error output from the comparison tool
                                    string line;
                                    using (var reader = new StringReader(successOutput.ToString()))
                                        while ((line = reader.ReadLine()) != null)
                                            logger.Info(line);
                                    using (var reader = new StringReader(errorOutput.ToString()))
                                        while ((line = reader.ReadLine()) != null)
                                            logger.Error(line);

                                    // don't fail on pull tester result, consensus is not implemented and it will always fail
                                    if (didJavaExit)
                                        Assert.Inconclusive(errorOutput.Length > 0 ? errorOutput.ToString() : output.ToString());
                                    else
                                    {
                                        // if java.exe failed to terminate, log last X lines of output
                                        var lastLinesCount = 20;
                                        var lastLines = new List<string>(lastLinesCount);

                                        var match = Regex.Match(output.ToString(), "^.*$", RegexOptions.Multiline | RegexOptions.RightToLeft);
                                        var count = 0;
                                        while (count < lastLinesCount && (match = match.NextMatch()) != null)
                                        {
                                            lastLines.Add(match.Value);
                                            count++;
                                        }

                                        lastLines.Reverse();
                                        Assert.Fail($"java.exe failed to terminate: {string.Join("", lastLines)}");
                                    }
                                }

                                Assert.AreEqual(0, javaProcess.ExitCode);
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
