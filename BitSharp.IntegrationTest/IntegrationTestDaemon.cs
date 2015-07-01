using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Script;
using BitSharp.Core.Storage;
using BitSharp.Core.Storage.Memory;
using BitSharp.Core.Test.Rules;
using BitSharp.Esent;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Ninject;
using NLog;
using Org.BouncyCastle.Crypto.Parameters;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using BitSharp.IntegrationTest;
using BitSharp.Node;
using BitSharp.Lmdb;

namespace BitSharp.Core.Test
{
    public class IntegrationTestDaemon : IDisposable
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

        private IDisposable baseDirectoryCleanup;
        private string baseDirectory;

        private bool isDisposed;

        public IntegrationTestDaemon(Block genesisBlock = null, bool useLmdb = false)
        {
            // initialize storage folder
            this.baseDirectoryCleanup = TempDirectory.CreateTempDirectory(out this.baseDirectory);

            // initialize kernel
            this.kernel = new StandardKernel();

            // add logging module
            this.kernel.Load(new LoggingModule(this.baseDirectory, LogLevel.Info));

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
            this.kernel.Load(new EsentStorageModule(baseDirectory, RulesEnum.TestNet2, blockStorage: !useLmdb, cacheSizeMaxBytes: 500.MILLION()));
            if (useLmdb)
                this.kernel.Load(new LmdbStorageModule(baseDirectory, RulesEnum.TestNet2));

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
                this.coreDaemon.Dispose();
                this.kernel.Dispose();
                this.baseDirectoryCleanup.Dispose();

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

        public Block CreateLargeBlock(UInt256 previousBlockHash, int txCount, UInt256 target = null)
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
                        scriptSignature: previousBlockHash.ToByteArray().Concat(random.NextBytes(100)),
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


            var transactionsBuilder = ImmutableArray.CreateBuilder<Transaction>(txCount);
            transactionsBuilder.Add(coinbaseTx);

            var prevTx = coinbaseTx;
            for (var i = 1; i < transactionsBuilder.Capacity; i++)
            {
                var outputs =
                    i % 2 == 0 ?
                    ImmutableArray.Create(
                        new TxOutput(prevTx.Outputs[0].Value - 1, coinbaseTx.Outputs[0].ScriptPublicKey),
                        new TxOutput(1, coinbaseTx.Outputs[0].ScriptPublicKey))
                    :
                    ImmutableArray.Create(new TxOutput(prevTx.Outputs[0].Value - 1, coinbaseTx.Outputs[0].ScriptPublicKey));

                var tx = new Transaction(
                    version: 0,
                    inputs: ImmutableArray.Create(new TxInput(new TxOutputKey(prevTx.Hash, 0), new byte[100], 0)),
                    outputs: outputs,
                    lockTime: 0);

                transactionsBuilder.Add(tx);
                prevTx = tx;
            }

            //Debug.WriteLine("Coinbase Tx Created: {0}".Format2(coinbaseTx.Hash.ToHexNumberString()));

            var transactions = transactionsBuilder.MoveToImmutable();

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

        public Block MineLargeBlock(UInt256 previousBlockHash, int txCount, UInt256 target = null)
        {
            return MineBlock(CreateLargeBlock(previousBlockHash, txCount, target));
        }

        public Block MineAndAddLargeBlock(UInt256 previousBlockHash, int txCount, UInt256 target = null)
        {
            var block = MineLargeBlock(previousBlockHash, txCount, target);
            if (previousBlockHash == UInt256.Zero)
            {
                this.rules.SetGenesisBlock(block);
                this.coreStorage.AddGenesisBlock(this.rules.GenesisChainedHeader);
            }
            
            AddBlock(block);
            return block;
        }

        public Block MineAndAddLargeBlock(Block prevBlock, int txCount, UInt256 target = null)
        {
            return MineAndAddLargeBlock(prevBlock.Hash, txCount, target);
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
