using BitSharp.Common;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using BitSharp.Core.Test.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BitSharp.Core.Storage;
using Moq;

namespace BitSharp.Core.Test.Builders
{
    [TestClass]
    public class UnconfirmedTxesBuilderTest
    {
        private MemoryTestStorageProvider storageProvider;
        private IStorageManager storageManager;

        [TestInitialize]
        public void Initialize()
        {
            storageProvider = new MemoryTestStorageProvider();
            storageManager = storageProvider.OpenStorageManager();
        }

        [TestCleanup]
        public void Cleanup()
        {
            storageProvider?.TestCleanup();
            storageManager?.Dispose();

            storageProvider = null;
            storageManager = null;
        }

        [TestMethod]
        public void TestUnconfTxAdded()
        {
            // create tx spending a previous output that exists
            var decodedTx = Transaction.Create(
                0,
                ImmutableArray.Create(new TxInput(UInt256.One, 0, ImmutableArray<byte>.Empty, 0)),
                ImmutableArray.Create(new TxOutput(0, ImmutableArray<byte>.Empty)),
                0);
            var tx = decodedTx.Transaction;

            // create prev output tx
            var unspentTx = new UnspentTx(tx.Inputs[0].PrevTxHash, 0, 1, 0, false, new OutputStates(1, OutputState.Unspent));
            var txOutput = new TxOutput(0, ImmutableArray<byte>.Empty);

            // mock chain state with prev output
            var chainState = new Mock<IChainState>();
            chainState.Setup(x => x.TryGetUnspentTx(tx.Inputs[0].PrevTxHash, out unspentTx)).Returns(true);
            chainState.Setup(x => x.TryGetUnspentTxOutput(tx.Inputs[0].PrevTxOutputKey, out txOutput)).Returns(true);

            // mock core daemon for chain state retrieval
            var coreDaemon = new Mock<ICoreDaemon>();
            coreDaemon.Setup(x => x.GetChainState()).Returns(chainState.Object);

            using (var unconfirmedTxesBuilder = new UnconfirmedTxesBuilder(coreDaemon.Object, Mock.Of<ICoreStorage>(), storageManager))
            {
                // try to add the tx
                Assert.IsTrue(unconfirmedTxesBuilder.TryAddTransaction(decodedTx));

                // verify unconfirmed tx was added
                UnconfirmedTx unconfirmedTx;
                Assert.IsTrue(unconfirmedTxesBuilder.TryGetTransaction(tx.Hash, out unconfirmedTx));
                Assert.IsNotNull(unconfirmedTx);

                // verify tx was indexed against its input
                var txesSpending = unconfirmedTxesBuilder.GetTransactionsSpending(tx.Inputs[0].PrevTxOutputKey);
                Assert.AreEqual(1, txesSpending.Count);
                Assert.AreEqual(tx.Hash, txesSpending.Values.Single().Hash);
            }
        }

        [TestMethod]
        public void TestUnconfTxMissingPrevOutput()
        {
            // create tx spending a previous output that doesn't exist
            var decodedTx = Transaction.Create(
                0,
                ImmutableArray.Create(new TxInput(UInt256.One, 0, ImmutableArray<byte>.Empty, 0)),
                ImmutableArray.Create(new TxOutput(0, ImmutableArray<byte>.Empty)),
                0);
            var tx = decodedTx.Transaction;

            // mock empty chain state
            var chainState = new Mock<IChainState>();

            // mock core daemon for chain state retrieval
            var coreDaemon = new Mock<ICoreDaemon>();
            coreDaemon.Setup(x => x.GetChainState()).Returns(chainState.Object);

            using (var unconfirmedTxesBuilder = new UnconfirmedTxesBuilder(coreDaemon.Object, Mock.Of<ICoreStorage>(), storageManager))
            {
                // try to add the tx
                Assert.IsFalse(unconfirmedTxesBuilder.TryAddTransaction(decodedTx));

                // verify unconfirmed tx was not added
                UnconfirmedTx unconfirmedTx;
                Assert.IsFalse(unconfirmedTxesBuilder.TryGetTransaction(tx.Hash, out unconfirmedTx));
                Assert.IsNull(unconfirmedTx);

                // verify tx is not indexed against its input
                Assert.AreEqual(0, unconfirmedTxesBuilder.GetTransactionsSpending(tx.Inputs[0].PrevTxOutputKey).Count);
            }
        }

        [TestMethod]
        public void TestUnconfTxPrevOutputSpent()
        {
            // create tx spending a previous output that exists, but is spent
            var decodedTx = Transaction.Create(
                0,
                ImmutableArray.Create(new TxInput(UInt256.One, 0, ImmutableArray<byte>.Empty, 0)),
                ImmutableArray.Create(new TxOutput(0, ImmutableArray<byte>.Empty)),
                0);
            var tx = decodedTx.Transaction;

            // create prev output tx
            var unspentTx = new UnspentTx(tx.Inputs[0].PrevTxHash, 0, 1, 0, false, new OutputStates(1, OutputState.Spent));

            // mock chain state with prev output
            var chainState = new Mock<IChainState>();
            chainState.Setup(x => x.TryGetUnspentTx(tx.Inputs[0].PrevTxHash, out unspentTx)).Returns(true);

            // mock core daemon for chain state retrieval
            var coreDaemon = new Mock<ICoreDaemon>();
            coreDaemon.Setup(x => x.GetChainState()).Returns(chainState.Object);

            using (var unconfirmedTxesBuilder = new UnconfirmedTxesBuilder(coreDaemon.Object, Mock.Of<ICoreStorage>(), storageManager))
            {
                // try to add the tx
                Assert.IsFalse(unconfirmedTxesBuilder.TryAddTransaction(decodedTx));

                // verify unconfirmed tx was not added
                UnconfirmedTx unconfirmedTx;
                Assert.IsFalse(unconfirmedTxesBuilder.TryGetTransaction(tx.Hash, out unconfirmedTx));
                Assert.IsNull(unconfirmedTx);

                // verify tx is not indexed against its input
                Assert.AreEqual(0, unconfirmedTxesBuilder.GetTransactionsSpending(tx.Inputs[0].PrevTxOutputKey).Count);
            }
        }

        [TestMethod]
        public void TestAddBlockConfirmingTx()
        {
            // create tx spending a previous output that exists
            var decodedTx = Transaction.Create(
                0,
                ImmutableArray.Create(new TxInput(UInt256.One, 0, ImmutableArray<byte>.Empty, 0)),
                ImmutableArray.Create(new TxOutput(0, ImmutableArray<byte>.Empty)),
                0);
            var tx = decodedTx.Transaction;

            // create prev output tx
            var unspentTx = new UnspentTx(tx.Inputs[0].PrevTxHash, 0, 1, 0, false, new OutputStates(1, OutputState.Unspent));
            var txOutput = new TxOutput(0, ImmutableArray<byte>.Empty);

            // create a fake chain
            var fakeHeaders = new FakeHeaders();
            var genesisHeader = fakeHeaders.GenesisChained();

            // create a block confirming the tx
            var block = Block.Create(RandomData.RandomBlockHeader().With(PreviousBlock: genesisHeader.Hash), ImmutableArray.Create(tx));
            var chainedHeader = new ChainedHeader(block.Header, 1, 0, DateTimeOffset.Now);

            // mock core storage with chained header
            var coreStorage = new Mock<ICoreStorage>();
            var initialChain = new ChainBuilder().ToImmutable();
            coreStorage.Setup(x => x.TryReadChain(null, out initialChain)).Returns(true);
            coreStorage.Setup(x => x.TryGetChainedHeader(chainedHeader.Hash, out chainedHeader)).Returns(true);

            // mock chain state with prev output
            var chainState = new Mock<IChainState>();
            chainState.Setup(x => x.TryGetUnspentTx(tx.Inputs[0].PrevTxHash, out unspentTx)).Returns(true);
            chainState.Setup(x => x.TryGetUnspentTxOutput(tx.Inputs[0].PrevTxOutputKey, out txOutput)).Returns(true);

            // mock core daemon for chain state retrieval
            var coreDaemon = new Mock<ICoreDaemon>();
            coreDaemon.Setup(x => x.GetChainState()).Returns(chainState.Object);

            using (var unconfirmedTxesBuilder = new UnconfirmedTxesBuilder(coreDaemon.Object, coreStorage.Object, storageManager))
            {
                // add the tx
                Assert.IsTrue(unconfirmedTxesBuilder.TryAddTransaction(decodedTx));

                // add the block
                unconfirmedTxesBuilder.AddBlock(genesisHeader, Enumerable.Empty<BlockTx>());
                unconfirmedTxesBuilder.AddBlock(chainedHeader, block.BlockTxes);

                // verify the confirmed tx was removed
                UnconfirmedTx unconfirmedTx;
                Assert.IsFalse(unconfirmedTxesBuilder.TryGetTransaction(tx.Hash, out unconfirmedTx));
                Assert.IsNull(unconfirmedTx);

                // verify the confirmed tx was de-indexed against its input
                Assert.AreEqual(0, unconfirmedTxesBuilder.GetTransactionsSpending(tx.Inputs[0].PrevTxOutputKey).Count);
            }
        }
    }
}
