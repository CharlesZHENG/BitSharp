﻿using BitSharp.Blockchain;
using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Script;
using BitSharp.Core.Storage;
using BitSharp.Core.Storage.Memory;
using BitSharp.Core.Test.Rules;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Ninject;
using NLog;
using Org.BouncyCastle.Crypto.Parameters;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Core.Test
{
    public class MainnetSimulator : IDisposable
    {
        private const UInt64 SATOSHI_PER_BTC = 100 * 1000 * 1000;

        private readonly Random random;
        private readonly MainnetBlockProvider blockProvider;
        private readonly IKernel kernel;
        private readonly Logger logger;
        private readonly CoreDaemon coreDaemon;
        private readonly CoreStorage coreStorage;

        public MainnetSimulator()
        {
            this.random = new Random();
            this.blockProvider = new MainnetBlockProvider();

            // initialize kernel
            this.kernel = new StandardKernel();

            // add logging module
            this.kernel.Load(new ConsoleLoggingModule());

            // log startup
            this.logger = kernel.Get<Logger>();
            this.logger.Info("Starting up: {0}".Format2(DateTime.Now));

            // add storage module
            this.kernel.Load(new MemoryStorageModule());

            // add rules module
            this.kernel.Load(new RulesModule(RulesEnum.MainNet));

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
            Assert.AreEqual(rules.GenesisBlock.Hash, this.coreDaemon.TargetChain.LastBlockHash);
            Assert.AreEqual(rules.GenesisBlock.Hash, this.coreDaemon.CurrentChain.LastBlockHash);
        }

        public void Dispose()
        {
            this.kernel.Dispose();
        }

        public MainnetBlockProvider BlockProvider { get { return this.blockProvider; } }

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
            var block = this.blockProvider.GetBlock(blockHash.ToHexNumberString());
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
}
