using BitSharp.Common.ExtensionMethods;
using BitSharp.Core;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage.Memory;
using BitSharp.Esent;
using BitSharp.LevelDb;
using BitSharp.Network;
using BitSharp.Network.Storage.Memory;
using BitSharp.Network.Workers;
using Ninject;
using Ninject.Modules;
using NLog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace BitSharp.Node
{
    public class BitSharpNode : IDisposable
    {
        private readonly Logger logger;

        private bool disposed;

        //TODO use config
        private readonly bool connectToPeers = true;

        public BitSharpNode()
        {
            //TODO
            //**************************************************************
            var useTestNet = false;
            var connectToPeers = true;

            var ignoreScripts = false;

            var pruningMode = PruningMode.TxIndex | PruningMode.BlockSpentIndex | PruningMode.BlockTxesDelete;

            var useLevelDb = false;
            var runInMemory = false;

            var cleanData = false;
            var cleanChainState = false;
            var cleanBlockTxes = false
                // clean block txes if the chain state is being cleaned and block txes have been pruned, the new chain state will require intact blocks to validate
                || (cleanChainState && (pruningMode.HasFlag(PruningMode.BlockTxesPreserveMerkle) || pruningMode.HasFlag(PruningMode.BlockTxesDestroyMerkle)));
            //NOTE: Running with a cleaned chained state against a pruned blockchain does not work.
            //      It will see the data is missing, but won't redownload the blocks.
            //**************************************************************

            long? cacheSizeMaxBytes = 512.MEBIBYTE();

            // directories
            var baseDirectory = Config.LocalStoragePath;
            //if (Debugger.IsAttached)
            //    baseDirectory = Path.Combine(baseDirectory, "Debugger");

            var chainType = useTestNet ? ChainType.TestNet3 : ChainType.MainNet;

            string[] blockTxesStorageLocations = null;

            // detect local dev machine - TODO proper configuration
            var isAzureVM = (Environment.MachineName == "BITSHARP");
            var isLocalDev = (Environment.MachineName == "SKIPPY");

            if (isAzureVM)
            {
                cacheSizeMaxBytes = null;
                BlockRequestWorker.SecondaryBlockFolder = @"E:\BitSharp.Blocks\RawBlocks";
                PeerWorker.ConnectedMax = 15;

                blockTxesStorageLocations = new[]
                {
                        @"E:\Blocks1",
                        @"E:\Blocks2",
                        @"E:\Blocks3",
                        @"E:\Blocks4",
                    };
            }
            else if (isLocalDev)
            {
                //Process.GetCurrentProcess().PriorityClass = ProcessPriorityClass.BelowNormal;

                cacheSizeMaxBytes = (int)2.GIBIBYTE();

                // location to store a copy of raw blocks to avoid redownload
                BlockRequestWorker.SecondaryBlockFolder = Path.Combine(baseDirectory, "RawBlocks");

                // split block txes storage across 2 dedicated SSDs, keep chain state on main SSD
                //blockTxesStorageLocations = new[]
                //{
                //    @"Y:\BitSharp",
                //    @"Z:\BitSharp",
                //};
            }

            //TODO
            if (cleanData)
            {
                try { Directory.Delete(Path.Combine(baseDirectory, "Data", chainType.ToString()), recursive: true); }
                catch (IOException) { }
            }
            if (cleanChainState)
            {
                try { Directory.Delete(Path.Combine(baseDirectory, "Data", chainType.ToString(), "ChainState"), recursive: true); }
                catch (IOException) { }
            }
            if (cleanBlockTxes)
            {
                try { Directory.Delete(Path.Combine(baseDirectory, "Data", chainType.ToString(), "BlockTxes"), recursive: true); }
                catch (IOException) { }
            }

            // initialize kernel
            this.Kernel = new StandardKernel();

            // add logging module
            this.Kernel.Load(new LoggingModule(baseDirectory, LogLevel.Info));

            // log startup
            this.logger = LogManager.GetCurrentClassLogger();
            this.logger.Info($"Starting up: {DateTime.Now}");

            var modules = new List<INinjectModule>();

            // add storage module
            if (useLevelDb)
            {
                ulong? blocksCacheSize = null;
                ulong? blocksWriteCacheSize = null;

                ulong? blockTxesCacheSize = null;
                ulong? blockTxesWriteCacheSize = null;

                ulong? chainStateCacheSize = null;
                ulong? chainStateWriteCacheSize = null;

                modules.Add(new LevelDbStorageModule(baseDirectory, chainType,
                    blocksCacheSize, blocksWriteCacheSize,
                    blockTxesCacheSize, blockTxesWriteCacheSize,
                    chainStateCacheSize, chainStateWriteCacheSize));

                long? writeCacheSizeMaxBytes = 128.MEBIBYTE();

            }
            else if (runInMemory)
            {
                modules.Add(new MemoryStorageModule());
                modules.Add(new NetworkMemoryStorageModule());
            }
            else
            {
                modules.Add(new EsentStorageModule(baseDirectory, chainType, cacheSizeMaxBytes: cacheSizeMaxBytes, blockTxesStorageLocations: blockTxesStorageLocations));
            }

            // add rules module
            modules.Add(new RulesModule(chainType));

            // load modules
            this.Kernel.Load(modules.ToArray());

            // initialize rules
            var rules = this.Kernel.Get<ICoreRules>();
            rules.IgnoreScripts = ignoreScripts;

            // initialize the blockchain daemon
            this.CoreDaemon = this.Kernel.Get<CoreDaemon>();
            this.CoreDaemon.PruningMode = pruningMode;
            this.Kernel.Bind<CoreDaemon>().ToConstant(this.CoreDaemon).InTransientScope();

            // initialize p2p client
            this.LocalClient = this.Kernel.Get<LocalClient>();
            this.Kernel.Bind<LocalClient>().ToConstant(this.LocalClient).InTransientScope();
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
                Task.Run(() => this.CoreDaemon.Start()),
                // start p2p client
                Task.Run(() => this.LocalClient.Start(connectToPeers)));
        }
    }
}
