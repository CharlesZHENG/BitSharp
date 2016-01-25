using BitSharp.Common.ExtensionMethods;
using BitSharp.Core;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage.Memory;
using BitSharp.Esent;
using BitSharp.LevelDb;
using BitSharp.Network;
using BitSharp.Network.Storage.Memory;
using BitSharp.Network.Workers;
using CommandLine;
using Ninject;
using NLog;
using SharpConfig;
using System;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;

namespace BitSharp.Node
{
    public class BitSharpNode : IDisposable
    {
        private readonly Logger logger;

        private readonly NodeConfig nodeConfig;

        private bool disposed;

        public static void Main(string[] args)
        {
            try
            {
                using (var bitSharpNode = new BitSharpNode(args, strictArgs: true))
                {
                    bitSharpNode.StartAsync().Forget();

                    Console.WriteLine("Press enter to exit node.");
                    Console.ReadLine();
                    Console.WriteLine("Shutting down.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                Environment.Exit(-1);
            }
        }

        public BitSharpNode(string[] args, bool strictArgs)
        {
            // parse command line options
            var options = new NodeOptions();

            var parser = new Parser(settings =>
            {
                settings.HelpWriter = Parser.Default.Settings.HelpWriter;
                settings.MutuallyExclusive = true;
            });

            if (strictArgs)
            {
                parser.ParseArgumentsStrict(args, options);
            }
            else
            {
                if (!parser.ParseArguments(args, options))
                    throw new InvalidOperationException($"Invalid command line:\n {options.GetUsage()}");
            }

            options.DataFolder = Environment.ExpandEnvironmentVariables(options.DataFolder);

            // clean data folder, depending on options
            if (Directory.Exists(options.DataFolder))
            {
                if (options.Clean || options.CleanAll)
                {
                    var dataPath = Path.Combine(options.DataFolder, "Data");
                    var peerPath = Path.Combine(options.DataFolder, "Peer");

                    if (Directory.Exists(dataPath))
                        Directory.Delete(dataPath, recursive: true);
                    if (Directory.Exists(peerPath))
                        Directory.Delete(peerPath, recursive: true);
                }

                if (options.CleanAll)
                {
                    var iniPath = Path.Combine(options.DataFolder, "BitSharp.ini");
                    var logPath = Path.Combine(options.DataFolder, "BitSharp.log");

                    if (File.Exists(iniPath))
                        File.Delete(iniPath);
                    if (File.Exists(logPath))
                        File.Delete(logPath);
                }
            }

            // create data folder
            Directory.CreateDirectory(options.DataFolder);

            // initialize kernel
            Kernel = new StandardKernel();
            try
            {
                // add logging module
                Kernel.Load(new LoggingModule(options.DataFolder, LogLevel.Info));

                // log startup
                logger = LogManager.GetCurrentClassLogger();
                logger.Info($"Starting up: {DateTime.Now}");
                logger.Info($"Using data folder: {options.DataFolder}");

                // write default ini file, if it doesn't exist
                var iniFile = Path.Combine(options.DataFolder, "BitSharp.ini");
                if (!File.Exists(iniFile))
                {
                    var assembly = Assembly.GetAssembly(GetType());
                    using (var defaultIniStream = assembly.GetManifestResourceStream("BitSharp.Node.BitSharp.ini"))
                    using (var outputStream = File.Create(iniFile))
                    {
                        defaultIniStream.CopyTo(outputStream);
                    }
                }

                logger.Info($"Loading configuration: {iniFile}");

                // parse ini
                var iniConfig = Configuration.LoadFromFile(iniFile);

                // parse node config
                if (!iniConfig.Contains("Node"))
                    throw new ApplicationException("INI is missing [Node] section");

                nodeConfig = iniConfig["Node"].CreateObject<NodeConfig>();

                // parse dev config
                if (iniConfig.Contains("Dev"))
                {
                    var devConfig = iniConfig["Dev"].CreateObject<DevConfig>();

                    if (devConfig.SecondaryBlockFolder != null)
                    {
                        BlockRequestWorker.SecondaryBlockFolder =
                            Environment.ExpandEnvironmentVariables(devConfig.SecondaryBlockFolder);
                    }
                }

                // add storage module
                switch (nodeConfig.StorageType)
                {
                    case StorageType.Esent:
                        if (!iniConfig.Contains("Esent"))
                            throw new ApplicationException("INI is missing [Esent] section");

                        var esentConfig = iniConfig["Esent"].CreateObject<EsentConfig>();

                        var cacheSizeMaxBytes = esentConfig.CacheSizeMaxMebiBytes != null
                            ? esentConfig.CacheSizeMaxMebiBytes * 1.MEBIBYTE() : null;

                        Kernel.Load(new EsentStorageModule(options.DataFolder, nodeConfig.ChainType, cacheSizeMaxBytes));
                        break;

                    case StorageType.LevelDb:
                        Kernel.Load(new LevelDbStorageModule(options.DataFolder, nodeConfig.ChainType));
                        break;

                    case StorageType.Memory:
                        Kernel.Load(new MemoryStorageModule());
                        Kernel.Load(new NetworkMemoryStorageModule());
                        break;

                    default:
                        throw new ApplicationException($"INI has unrecognized storage type: {nodeConfig.StorageType}");
                }

                // add rules module
                Kernel.Load(new RulesModule(nodeConfig.ChainType));

                // initialize rules
                var rules = Kernel.Get<ICoreRules>();
                rules.IgnoreScripts = !nodeConfig.ExecuteScripts;

                // initialize the blockchain daemon
                CoreDaemon = Kernel.Get<CoreDaemon>();
                CoreDaemon.PruningMode = nodeConfig.PruningMode;
                Kernel.Bind<CoreDaemon>().ToConstant(CoreDaemon).InTransientScope();

                // initialize p2p client
                LocalClient = Kernel.Get<LocalClient>();
                Kernel.Bind<LocalClient>().ToConstant(LocalClient).InTransientScope();
            }
            catch (Exception)
            {
                Kernel.Dispose();
                throw;
            }
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
                // shutdown
                LocalClient.Dispose();
                CoreDaemon.Dispose();
                Kernel.Dispose();

                disposed = true;
            }
        }

        public IKernel Kernel { get; }

        public CoreDaemon CoreDaemon { get; }

        public LocalClient LocalClient { get; }

        public async Task StartAsync()
        {
            await Task.WhenAll(
                // start the blockchain daemon
                Task.Run(() => CoreDaemon.Start()),
                // start p2p client
                Task.Run(() => LocalClient.Start(nodeConfig.ConnectToPeers)));
        }
    }
}
