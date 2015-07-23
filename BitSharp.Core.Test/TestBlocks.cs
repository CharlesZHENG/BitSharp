using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using BitSharp.Core.Script;
using BitSharp.Core.Test.Rules;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NLog;
using Org.BouncyCastle.Crypto.Parameters;
using System;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;

namespace BitSharp.Core.Test
{
    public class TestBlocks
    {
        private const UInt64 SATOSHI_PER_BTC = 100 * 1000 * 1000;

        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly Random random = new Random();

        private readonly TransactionManager txManager = new TransactionManager();
        private readonly ECPrivateKeyParameters coinbasePrivateKey;
        private readonly ECPublicKeyParameters coinbasePublicKey;

        private readonly Miner miner = new Miner();
        private readonly UnitTestRules rules;

        private readonly ImmutableList<Block>.Builder blocks = ImmutableList.CreateBuilder<Block>();
        private readonly ChainBuilder chain = new ChainBuilder();

        public TestBlocks(Block genesisBlock = null)
        {
            // create the key pair that block rewards will be sent to
            var keyPair = txManager.CreateKeyPair();
            coinbasePrivateKey = keyPair.Item1;
            coinbasePublicKey = keyPair.Item2;

            // create and mine the genesis block
            genesisBlock = genesisBlock ?? MineEmptyBlock(UInt256.Zero);

            // initialize unit test rules
            rules = new UnitTestRules()
            {
                // disable execution of rules validation
                ValidateTransactionAction = (_, __) => { },
                ValidationTransactionScriptAction = (_, __, ___, ____, _____, ______) => { }
            };
            rules.SetGenesisBlock(genesisBlock);

            blocks.Add(genesisBlock);
            chain.AddBlock(rules.GenesisChainedHeader);
        }

        public TestBlocks(TestBlocks parent)
        {
            this.random = parent.random;
            this.txManager = parent.txManager;
            this.coinbasePrivateKey = parent.coinbasePrivateKey;
            this.coinbasePublicKey = parent.coinbasePublicKey;

            this.miner = parent.miner;
            this.rules = parent.rules;

            this.blocks = parent.blocks.ToImmutable().ToBuilder();
            this.chain = parent.chain.ToImmutable().ToBuilder();
        }

        public TransactionManager TxManager => txManager;

        public ECPrivateKeyParameters CoinbasePrivateKey => coinbasePrivateKey;

        public ECPublicKeyParameters CoinbasePublicKey => coinbasePublicKey;

        public Miner Miner => miner;

        public Block GenesisBlock => blocks.First();

        public ImmutableList<Block> Blocks => blocks.ToImmutable();

        public Chain Chain => chain.ToImmutable();

        public UnitTestRules Rules => rules;

        public Block CreateBlock(UInt256 previousBlockHash, int txCount, UInt256 target = null)
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
                        scriptSignature: previousBlockHash.ToByteArray().Concat(random.NextBytes(100)).ToImmutableArray(),
                        sequence: 0
                    )
                ),
                outputs: ImmutableArray.Create
                (
                    new TxOutput
                    (
                        value: 50 * SATOSHI_PER_BTC,
                        scriptPublicKey: this.txManager.CreatePublicKeyScript(coinbasePublicKey).ToImmutableArray()
                    )
                ),
                lockTime: 0
            );


            var transactionsBuilder = ImmutableArray.CreateBuilder<Transaction>(txCount + 1);
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
                    inputs: ImmutableArray.Create(new TxInput(new TxOutputKey(prevTx.Hash, 0), new byte[100].ToImmutableArray(), 0)),
                    outputs: outputs,
                    lockTime: 0);

                transactionsBuilder.Add(tx);
                prevTx = tx;
            }

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

        public Block CreateEmptyBlock(UInt256 prevBlockHash, UInt256 target = null)
        {
            return CreateBlock(prevBlockHash, 0, target);
        }

        public Block MineBlock(Block block)
        {
            var minedHeader = this.miner.MineBlockHeader(block.Header, DataCalculator.BitsToTarget(block.Header.Bits));
            if (minedHeader == null)
                Assert.Fail("No block could be mined for test data header.");

            block = block.With(Header: minedHeader);

            return block;
        }

        public Block MineBlock(UInt256 prevBlockHash, int txCount, UInt256 target = null)
        {
            return MineBlock(CreateBlock(prevBlockHash, txCount, target));
        }

        public Block MineEmptyBlock(UInt256 prevBlockHash, UInt256 target = null)
        {
            return MineBlock(CreateBlock(prevBlockHash, 0, target));
        }

        public Block MineAndAddEmptyBlock(UInt256 target = null)
        {
            var prevBlockHash = blocks.Last().Hash;
            var block = MineBlock(prevBlockHash, 0, target);
            AddBlock(block);
            return block;
        }

        public Block MineAndAddBlock(int txCount, UInt256 target = null)
        {
            var prevBlockHash = blocks.Last().Hash;
            var block = MineBlock(prevBlockHash, txCount, target);
            AddBlock(block);
            return block;
        }

        public Block MineAndAddBlock(Block newBlock, UInt256 target = null)
        {
            var block = MineBlock(newBlock);
            AddBlock(block);
            return block;
        }

        public void AddBlock(Block block)
        {
            blocks.Add(block);
            chain.AddBlock(ChainedHeader.CreateFromPrev(chain.LastBlock, block.Header));

            Debug.Assert(chain.Height == blocks.Count - 1);
        }

        public void Rollback(int count)
        {
            if (count > blocks.Count)
                throw new InvalidOperationException();

            for (var i = 0; i < count; i++)
            {
                blocks.RemoveAt(blocks.Count - 1);
                chain.RemoveBlock(chain.LastBlock);
            }

            Debug.Assert(chain.Height == blocks.Count - 1);
        }

        public TestBlocks Fork(int rollbackCount = 0)
        {
            var fork = new TestBlocks(this);
            fork.Rollback(rollbackCount);
            return fork;
        }
    }
}
