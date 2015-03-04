using BitSharp.Common;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage.Memory;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NLog;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Core.Test.Builders
{
    [TestClass]
    public class UtxoBuilderTest
    {
        [TestMethod]
        public void TestSimpleSpend()
        {
            // prepare block
            var fakeHeaders = new FakeHeaders();
            var chainedHeader0 = fakeHeaders.GenesisChained();
            var chainedHeader1 = fakeHeaders.NextChained();
            var chainedHeader2 = fakeHeaders.NextChained();
            var chain = new ChainBuilder();
            var emptyCoinbaseTx0 = new Transaction(0, ImmutableArray.Create<TxInput>(), ImmutableArray.Create<TxOutput>(), 0);
            var emptyCoinbaseTx1 = new Transaction(1, ImmutableArray.Create<TxInput>(), ImmutableArray.Create<TxOutput>(), 0);
            var emptyCoinbaseTx2 = new Transaction(2, ImmutableArray.Create<TxInput>(), ImmutableArray.Create<TxOutput>(), 0);

            // initialize memory utxo builder storage
            var memoryStorage = new MemoryStorageManager();
            var memoryChainStateCursor = memoryStorage.OpenChainStateCursor().Item;

            // initialize utxo builder
            var utxoBuilder = new UtxoBuilder(memoryChainStateCursor);

            // prepare an unspent transaction
            var txHash = new UInt256(100);
            var unspentTx = new UnspentTx(txHash, chainedHeader0.Height, 0, 3, OutputState.Unspent);

            // prepare unspent output
            var unspentTransactions = ImmutableDictionary.Create<UInt256, UnspentTx>().Add(txHash, unspentTx);

            // add the unspent transaction
            memoryChainStateCursor.TryAddUnspentTx(unspentTx);

            // create an input to spend the unspent transaction's first output
            var input0 = new TxInput(new TxOutputKey(txHash, txOutputIndex: 0), ImmutableArray.Create<byte>(), 0);
            var tx0 = new Transaction(0, ImmutableArray.Create(input0), ImmutableArray.Create<TxOutput>(), 0);

            // spend the input
            chain.AddBlock(chainedHeader0);
            utxoBuilder.CalculateUtxo(chain.ToImmutable(), new[] { emptyCoinbaseTx0, tx0 }).ToList();

            // verify utxo storage
            UnspentTx actualUnspentTx;
            Assert.IsTrue(memoryChainStateCursor.TryGetUnspentTx(txHash, out actualUnspentTx));
            Assert.IsTrue(actualUnspentTx.OutputStates.Length == 3);
            Assert.IsTrue(actualUnspentTx.OutputStates[0] == OutputState.Spent);
            Assert.IsTrue(actualUnspentTx.OutputStates[1] == OutputState.Unspent);
            Assert.IsTrue(actualUnspentTx.OutputStates[2] == OutputState.Unspent);

            // create an input to spend the unspent transaction's second output
            var input1 = new TxInput(new TxOutputKey(txHash, txOutputIndex: 1), ImmutableArray.Create<byte>(), 0);
            var tx1 = new Transaction(0, ImmutableArray.Create(input1), ImmutableArray.Create<TxOutput>(), 0);

            // spend the input
            chain.AddBlock(chainedHeader1);
            utxoBuilder.CalculateUtxo(chain.ToImmutable(), new[] { emptyCoinbaseTx1, tx1 }).ToList();

            // verify utxo storage
            Assert.IsTrue(memoryChainStateCursor.TryGetUnspentTx(txHash, out actualUnspentTx));
            Assert.IsTrue(actualUnspentTx.OutputStates.Length == 3);
            Assert.IsTrue(actualUnspentTx.OutputStates[0] == OutputState.Spent);
            Assert.IsTrue(actualUnspentTx.OutputStates[1] == OutputState.Spent);
            Assert.IsTrue(actualUnspentTx.OutputStates[2] == OutputState.Unspent);

            // create an input to spend the unspent transaction's third output
            var input2 = new TxInput(new TxOutputKey(txHash, txOutputIndex: 2), ImmutableArray.Create<byte>(), 0);
            var tx2 = new Transaction(0, ImmutableArray.Create(input2), ImmutableArray.Create<TxOutput>(), 0);

            // spend the input
            chain.AddBlock(chainedHeader2);
            utxoBuilder.CalculateUtxo(chain.ToImmutable(), new[] { emptyCoinbaseTx2, tx2 }).ToList();

            // verify utxo storage
            Assert.IsTrue(memoryChainStateCursor.TryGetUnspentTx(txHash, out actualUnspentTx));
            Assert.IsTrue(actualUnspentTx.IsFullySpent);
        }

        [TestMethod]
        [ExpectedException(typeof(ValidationException))]
        public void TestDoubleSpend()
        {
            // prepare block
            var fakeHeaders = new FakeHeaders();
            var chainedHeader0 = fakeHeaders.GenesisChained();
            var chainedHeader1 = fakeHeaders.NextChained();
            var chain = new ChainBuilder();
            var emptyCoinbaseTx0 = new Transaction(0, ImmutableArray.Create<TxInput>(), ImmutableArray.Create<TxOutput>(), 0);
            var emptyCoinbaseTx1 = new Transaction(1, ImmutableArray.Create<TxInput>(), ImmutableArray.Create<TxOutput>(), 0);

            // initialize memory utxo builder storage
            var memoryStorage = new MemoryStorageManager();
            var memoryChainStateCursor = memoryStorage.OpenChainStateCursor().Item;

            // initialize utxo builder
            var utxoBuilder = new UtxoBuilder(memoryChainStateCursor);

            // prepare an unspent transaction
            var txHash = new UInt256(100);
            var unspentTx = new UnspentTx(txHash, chainedHeader0.Height, 0, 1, OutputState.Unspent);

            // add the unspent transaction
            memoryChainStateCursor.TryAddUnspentTx(unspentTx);

            // create an input to spend the unspent transaction
            var input = new TxInput(new TxOutputKey(txHash, txOutputIndex: 0), ImmutableArray.Create<byte>(), 0);
            var tx = new Transaction(0, ImmutableArray.Create(input), ImmutableArray.Create<TxOutput>(), 0);

            // spend the input
            chain.AddBlock(chainedHeader0);
            utxoBuilder.CalculateUtxo(chain.ToImmutable(), new[] { emptyCoinbaseTx0, tx }).ToList();

            // verify utxo storage
            UnspentTx actualUnspentTx;
            Assert.IsTrue(memoryChainStateCursor.TryGetUnspentTx(txHash, out actualUnspentTx));
            Assert.IsTrue(actualUnspentTx.IsFullySpent);

            // attempt to spend the input again
            chain.AddBlock(chainedHeader1);
            utxoBuilder.CalculateUtxo(chain.ToImmutable(), new[] { emptyCoinbaseTx1, tx }).ToList();

            // validation exception should be thrown
        }
    }
}
