using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage.Memory;
using BitSharp.Node;
using BitSharp.Node.Storage.Memory;
using Ninject;
using NLog;
using System;
using System.IO;
using System.IO.Compression;
using System.Threading;

namespace BitSharp.BlockHelper
{
    public class TestNet3Downloader
    {
        public static void Main(string[] args)
        {
            // initialize kernel
            using (var kernel = new StandardKernel())
            {
                // testnet data
                var desiredBlockHeight = 75.THOUSAND();
                Chain testNetChain = null;

                // add logging module
                kernel.Load(new ConsoleLoggingModule(LogLevel.Info));

                // log startup
                var logger = LogManager.GetCurrentClassLogger();
                logger.Info("Starting up: {0}".Format2(DateTime.Now));

                // determine local path of BitSharp.BlockHelper project
                var projectFolder = Environment.CurrentDirectory;
                while (!projectFolder.EndsWith(@"\BitSharp.BlockHelper", StringComparison.InvariantCultureIgnoreCase))
                    projectFolder = Path.GetDirectoryName(projectFolder);

                // prepare the block folder
                var blockFolder = Path.Combine(projectFolder, "Blocks");
                try { Directory.Delete(blockFolder, recursive: true); }
                catch (Exception) { }
                if (!Directory.Exists(blockFolder))
                    Directory.CreateDirectory(blockFolder);

                // add storage module
                kernel.Load(new MemoryStorageModule());
                kernel.Load(new NodeMemoryStorageModule());

                // add rules module
                var rulesType = RulesEnum.TestNet3;
                kernel.Load(new RulesModule(rulesType));

                // initialize the blockchain daemon
                using (var coreDaemon = kernel.Get<CoreDaemon>())
                {
                    kernel.Bind<CoreDaemon>().ToConstant(coreDaemon).InTransientScope();

                    // ignore script errors
                    var rules = kernel.Get<IBlockchainRules>();
                    rules.IgnoreScriptErrors = true;

                    // initialize p2p client
                    using (var localClient = kernel.Get<LocalClient>())
                    {
                        kernel.Bind<LocalClient>().ToConstant(localClient).InTransientScope();

                        // start the blockchain daemon
                        coreDaemon.Start();

                        // start p2p client
                        localClient.Start();

                        // wait for testnet chain to reach desired height
                        while (true)
                        {
                            using (var testNetChainState = coreDaemon.GetChainState())
                            {
                                logger.Info("TestNet blockchain at height: " + testNetChainState.Chain.Height);

                                if (testNetChainState.Chain.Height >= desiredBlockHeight)
                                {
                                    testNetChain = testNetChainState.Chain;
                                    break;
                                }
                                else
                                    Thread.Sleep(TimeSpan.FromSeconds(5));
                            }
                        }
                    }

                    // stop the daemon
                    coreDaemon.Stop();

                    // write testnet blocks out to disk
                    for (var height = 0; height <= desiredBlockHeight; height++)
                    {
                        if (height % 1000 == 0)
                            logger.Info("Writing block: {0:#,##0}".Format2(height));

                        var blockHash = testNetChain.Blocks[height].Hash;

                        Block block;
                        if (!coreDaemon.CoreStorage.TryGetBlock(blockHash, out block))
                            throw new Exception();

                        var blockFile = new FileInfo(Path.Combine(blockFolder, "{0:000000}_{1}.blk".Format2(height, block.Hash)));

                        using (var stream = new FileStream(blockFile.FullName, FileMode.Create))
                        using (var writer = new BinaryWriter(stream))
                        {
                            writer.Write(DataEncoder.EncodeBlock(block));
                        }
                    }

                    logger.Info("Writing zip file");

                    // update test data zip file
                    var destZipFile = Path.Combine(projectFolder, "..", "BitSharp.Core.Test", "Blocks.TestNet3.zip");
                    if (File.Exists(destZipFile))
                        File.Delete(destZipFile);
                    ZipFile.CreateFromDirectory(blockFolder, destZipFile);
                }
            }
        }
    }
}
