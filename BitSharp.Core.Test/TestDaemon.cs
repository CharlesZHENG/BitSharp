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
using Ninject.Modules;
using NLog;
using Org.BouncyCastle.Crypto.Parameters;
using System;

namespace BitSharp.Core.Test
{
    public class TestDaemon : IDisposable
    {
        private const UInt64 SATOSHI_PER_BTC = 100 * 1000 * 1000;

        //private readonly Random random;
        private readonly IKernel kernel;
        private readonly Logger logger;
        private readonly CoreDaemon coreDaemon;
        private readonly CoreStorage coreStorage;
        private readonly TestBlocks testBlocks;

        private IDisposable baseDirectoryCleanup;
        private string baseDirectory;

        private bool isDisposed;

        public TestDaemon(Block genesisBlock = null, INinjectModule loggingModule = null, INinjectModule[] storageModules = null)
        {
            // initialize storage folder
            this.baseDirectoryCleanup = TempDirectory.CreateTempDirectory(out this.baseDirectory);

            // initialize kernel
            this.kernel = new StandardKernel();

            // add logging module
            this.kernel.Load(loggingModule ?? new ConsoleLoggingModule());

            // log startup
            this.logger = LogManager.GetCurrentClassLogger();
            this.logger.Info("Starting up: {0}".Format2(DateTime.Now));

            // initialize test blocks
            this.testBlocks = new TestBlocks(genesisBlock);

            // add storage module
            this.kernel.Load(storageModules ?? new[] { new MemoryStorageModule() });

            // initialize unit test rules, allow validation methods to run
            testBlocks.Rules.ValidateTransactionAction = null;
            testBlocks.Rules.ValidationTransactionScriptAction = null;
            this.kernel.Bind<RulesEnum>().ToConstant(RulesEnum.TestNet2);
            this.kernel.Bind<IBlockchainRules>().ToConstant(testBlocks.Rules);

            // TODO ignore script errors in test daemon until scripting engine is completed
            testBlocks.Rules.IgnoreScriptErrors = true;

            // initialize the blockchain daemon
            this.kernel.Bind<CoreDaemon>().ToSelf().InSingletonScope();
            this.coreDaemon = this.kernel.Get<CoreDaemon>();
            try
            {
                this.coreStorage = this.coreDaemon.CoreStorage;

                // start the blockchain daemon
                this.coreDaemon.Start();

                // wait for initial work
                this.coreDaemon.WaitForUpdate();

                // verify initial state
                Assert.AreEqual(0, this.coreDaemon.TargetChainHeight);
                Assert.AreEqual(testBlocks.Rules.GenesisBlock.Hash, this.coreDaemon.TargetChain.LastBlock.Hash);
                Assert.AreEqual(testBlocks.Rules.GenesisBlock.Hash, this.coreDaemon.CurrentChain.LastBlock.Hash);
            }
            catch (Exception)
            {
                this.coreDaemon.Dispose();
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
            if (!isDisposed && disposing)
            {
                this.coreDaemon.Dispose();
                this.kernel.Dispose();
                this.baseDirectoryCleanup.Dispose();

                isDisposed = true;
            }
        }

        public IKernel Kernel { get { return this.kernel; } }

        public TransactionManager TxManager { get { return testBlocks.TxManager; } }

        public ECPrivateKeyParameters CoinbasePrivateKey { get { return testBlocks.CoinbasePrivateKey; } }

        public ECPublicKeyParameters CoinbasePublicKey { get { return testBlocks.CoinbasePublicKey; } }

        public Miner Miner { get { return testBlocks.Miner; } }

        public Block GenesisBlock { get { return testBlocks.GenesisBlock; } }

        public TestBlocks TestBlocks { get { return testBlocks; } }

        public UnitTestRules Rules { get { return testBlocks.Rules; } }

        public CoreDaemon CoreDaemon { get { return this.coreDaemon; } }

        public CoreStorage CoreStorage { get { return this.coreStorage; } }

        public Block CreateEmptyBlock(UInt256 prevBlockHash, UInt256 target = null)
        {
            return testBlocks.CreateEmptyBlock(prevBlockHash, target);
        }

        public Block MineAndAddEmptyBlock(UInt256 target = null)
        {
            var block = testBlocks.MineAndAddEmptyBlock(target);
            AddBlock(block);
            return block;
        }

        public Block MineAndAddBlock(int txCount, UInt256 target = null)
        {
            var block = testBlocks.MineAndAddBlock(txCount, target);
            AddBlock(block);
            return block;
        }

        public Block MineAndAddBlock(Block newBlock)
        {
            var block = testBlocks.MineAndAddBlock(newBlock);
            AddBlock(block);
            return block;
        }

        public Block AddBlock(Block block)
        {
            if (!this.coreStorage.TryAddBlock(block))
                Assert.Fail("Failed to store block: {0}".Format2(block.Hash));

            return block;
        }

        public void WaitForUpdate()
        {
            this.coreDaemon.WaitForUpdate();
        }

        public void AssertAtBlock(int expectedHeight, UInt256 expectedBlockHash)
        {
            Assert.AreEqual(expectedHeight, coreDaemon.TargetChain.Height);
            Assert.AreEqual(expectedBlockHash, coreDaemon.TargetChain.LastBlock.Hash);
            Assert.AreEqual(expectedHeight, coreDaemon.CurrentChain.Height);
            Assert.AreEqual(expectedBlockHash, coreDaemon.CurrentChain.LastBlock.Hash);
        }
    }
}
