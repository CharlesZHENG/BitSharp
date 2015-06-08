using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using BitSharp.Core.Storage.Memory;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Ninject;
using NLog;
using System;

namespace BitSharp.Core.Test
{
    public abstract class Simulator : IDisposable
    {
        private const UInt64 SATOSHI_PER_BTC = 100 * 1000 * 1000;

        private readonly Random random;
        private readonly BlockProvider blockProvider;
        private readonly IKernel kernel;
        private readonly Logger logger;
        private readonly CoreDaemon coreDaemon;
        private readonly CoreStorage coreStorage;

        public Simulator(RulesEnum rulesType)
        {
            // initialize kernel
            this.kernel = new StandardKernel();

            // add logging module
            this.kernel.Load(new ConsoleLoggingModule());

            // log startup
            this.logger = LogManager.GetCurrentClassLogger();
            this.logger.Info("Starting up: {0}".Format2(DateTime.Now));

            this.random = new Random();
            this.blockProvider = BlockProvider.CreateForRules(rulesType);

            // add storage module
            this.kernel.Load(new MemoryStorageModule());

            // add rules module
            this.kernel.Load(new RulesModule(rulesType));

            // TODO ignore script errors in test daemon until scripting engine is completed
            var rules = this.kernel.Get<IBlockchainRules>();
            rules.IgnoreScriptErrors = true;

            // initialize the blockchain daemon
            this.kernel.Bind<CoreDaemon>().ToSelf().InSingletonScope();
            this.coreDaemon = this.kernel.Get<CoreDaemon>();
            this.coreStorage = this.coreDaemon.CoreStorage;

            // start the blockchain daemon
            this.coreDaemon.Start();

            // wait for initial work
            this.coreDaemon.WaitForUpdate();

            // verify initial state
            Assert.AreEqual(0, this.coreDaemon.TargetChainHeight);
            Assert.AreEqual(rules.GenesisBlock.Hash, this.coreDaemon.TargetChain.LastBlock.Hash);
            Assert.AreEqual(rules.GenesisBlock.Hash, this.coreDaemon.CurrentChain.LastBlock.Hash);
        }

        public void Dispose()
        {
            this.kernel.Dispose();
            this.blockProvider.Dispose();
        }

        public BlockProvider BlockProvider { get { return this.blockProvider; } }

        public IKernel Kernel { get { return this.kernel; } }

        public CoreDaemon CoreDaemon { get { return this.coreDaemon; } }

        public void AddBlockRange(int fromHeight, int toHeight)
        {
            for (var height = fromHeight; height <= toHeight; height++)
            {
                var block = this.blockProvider.GetBlock(height);
                AddBlock(block);
            }
        }

        public void AddBlock(int height)
        {
            var block = this.blockProvider.GetBlock(height);
            AddBlock(block);
        }

        public void AddBlock(UInt256 blockHash)
        {
            var block = this.blockProvider.GetBlock(blockHash);
            AddBlock(block);
        }

        public void AddBlock(Block block)
        {
            this.coreStorage.TryAddBlock(block);
        }

        public void WaitForUpdate()
        {
            this.coreDaemon.WaitForUpdate();
        }
    }

    public class MainnetSimulator : Simulator
    {
        public MainnetSimulator()
            : base(RulesEnum.MainNet)
        {
        }
    }

    public class TestNet3Simulator : Simulator
    {
        public TestNet3Simulator()
            : base(RulesEnum.TestNet3)
        {
        }
    }
}
