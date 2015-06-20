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
using System.Collections.Immutable;

namespace BitSharp.Core.Test
{
    public class TestDaemon : IDisposable
    {
        private const UInt64 SATOSHI_PER_BTC = 100 * 1000 * 1000;

        private readonly Random random;
        private readonly IKernel kernel;
        private readonly Logger logger;
        private readonly TransactionManager txManager;
        private readonly ECPrivateKeyParameters coinbasePrivateKey;
        private readonly ECPublicKeyParameters coinbasePublicKey;
        private readonly Miner miner;
        private readonly Block genesisBlock;
        private readonly UnitTestRules rules;
        private readonly CoreDaemon coreDaemon;
        private readonly CoreStorage coreStorage;

        private bool isDisposed;

        public TestDaemon(Block genesisBlock = null)
        {
            // initialize kernel
            this.kernel = new StandardKernel();

            // add logging module
            this.kernel.Load(new ConsoleLoggingModule());

            // log startup
            this.logger = LogManager.GetCurrentClassLogger();
            this.logger.Info("Starting up: {0}".Format2(DateTime.Now));

            this.random = new Random();

            // create the key pair that block rewards will be sent to
            this.txManager = this.kernel.Get<TransactionManager>();
            var keyPair = this.txManager.CreateKeyPair();
            this.coinbasePrivateKey = keyPair.Item1;
            this.coinbasePublicKey = keyPair.Item2;

            // initialize miner
            this.miner = this.kernel.Get<Miner>();

            // create and mine the genesis block
            this.genesisBlock = genesisBlock ?? MineEmptyBlock(UInt256.Zero);

            // add storage module
            this.kernel.Load(new MemoryStorageModule());

            // initialize unit test rules
            this.rules = this.kernel.Get<UnitTestRules>();
            this.rules.SetGenesisBlock(this.genesisBlock);
            this.kernel.Bind<RulesEnum>().ToConstant(RulesEnum.TestNet2);
            this.kernel.Bind<IBlockchainRules>().ToConstant(rules);

            // TODO ignore script errors in test daemon until scripting engine is completed
            this.rules.IgnoreScriptErrors = true;

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
                Assert.AreEqual(this.genesisBlock.Hash, this.coreDaemon.TargetChain.LastBlock.Hash);
                Assert.AreEqual(this.genesisBlock.Hash, this.coreDaemon.CurrentChain.LastBlock.Hash);
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
                this.kernel.Dispose();

                isDisposed = true;
            }
        }

        public IKernel Kernel { get { return this.kernel; } }

        public TransactionManager TxManager { get { return this.txManager; } }

        public ECPrivateKeyParameters CoinbasePrivateKey { get { return this.coinbasePrivateKey; } }

        public ECPublicKeyParameters CoinbasePublicKey { get { return this.coinbasePublicKey; } }

        public Miner Miner { get { return this.miner; } }

        public Block GenesisBlock { get { return this.genesisBlock; } }

        public UnitTestRules Rules { get { return this.rules; } }

        public CoreDaemon CoreDaemon { get { return this.coreDaemon; } }

        public CoreStorage CoreStorage { get { return this.coreStorage; } }

        public Block CreateEmptyBlock(UInt256 previousBlockHash, UInt256 target = null)
        {
            var coinbaseTx = new Transaction
            (
                version: 0,
                inputs: ImmutableArray.Create
                (
                    new TxInput
                    (
                        previousTxOutputKey: new TxOutputKey
                        (
                            txHash: UInt256.Zero,
                            txOutputIndex: 0
                        ),
                        scriptSignature: random.NextBytes(100),
                        sequence: 0
                    )
                ),
                outputs: ImmutableArray.Create
                (
                    new TxOutput
                    (
                        value: 50 * SATOSHI_PER_BTC,
                        scriptPublicKey: this.txManager.CreatePublicKeyScript(coinbasePublicKey)
                    )
                ),
                lockTime: 0
            );

            //Debug.WriteLine("Coinbase Tx Created: {0}".Format2(coinbaseTx.Hash.ToHexNumberString()));

            var transactions = ImmutableArray.Create(coinbaseTx);
            var merkleRoot = MerkleTree.CalculateMerkleRoot(transactions);

            var block = new Block
            (
                header: new BlockHeader
                (
                    version: 0,
                    previousBlock: previousBlockHash,
                    merkleRoot: merkleRoot,
                    time: 0,
                    bits: DataCalculator.TargetToBits(target ?? UnitTestRules.Target0),
                    nonce: 0
                ),
                transactions: transactions
            );

            return block;
        }

        public Block CreateEmptyBlock(Block prevBlock, UInt256 target = null)
        {
            return CreateEmptyBlock(prevBlock.Hash, target);
        }

        public Block MineBlock(Block block)
        {
            var minedHeader = this.miner.MineBlockHeader(block.Header, DataCalculator.BitsToTarget(block.Header.Bits));
            if (minedHeader == null)
                Assert.Fail("No block could be mined for test data header.");

            block = block.With(Header: minedHeader);

            return block;
        }

        public Block MineEmptyBlock(UInt256 previousBlockHash, UInt256 target = null)
        {
            return MineBlock(CreateEmptyBlock(previousBlockHash, target));
        }

        public Block MineEmptyBlock(Block previousBlock, UInt256 target = null)
        {
            return MineEmptyBlock(previousBlock.Hash, target);
        }

        public Block MineAndAddEmptyBlock(UInt256 previousBlockHash, UInt256 target = null)
        {
            var block = MineEmptyBlock(previousBlockHash, target);
            AddBlock(block);
            return block;
        }

        public Block MineAndAddEmptyBlock(Block prevBlock, UInt256 target = null)
        {
            return MineAndAddEmptyBlock(prevBlock.Hash, target);
        }

        public Block MineAndAddBlock(Block block)
        {
            var minedHeader = this.miner.MineBlockHeader(block.Header, DataCalculator.BitsToTarget(block.Header.Bits));
            if (minedHeader == null)
                Assert.Fail("No block could be mined for test data header.");

            var minedBlock = block.With(Header: minedHeader);
            AddBlock(minedBlock);
            return minedBlock;
        }

        public void AddBlock(Block block)
        {
            if (!this.coreStorage.TryAddBlock(block))
                Assert.Fail("Failed to store block: {0}".Format2(block.Hash));
        }

        public void WaitForUpdate()
        {
            this.coreDaemon.WaitForUpdate();
        }
    }
}
