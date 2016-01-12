using BitSharp.Common;
using BitSharp.Common.Test;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage.Memory;
using Microsoft.VisualStudio.TestTools.UnitTesting;
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
            var chainedHeader3 = fakeHeaders.NextChained();
            var chain = Chain.CreateForGenesisBlock(chainedHeader0).ToBuilder();
            var emptyCoinbaseTx0 = BlockTx.Create(0, Transaction.Create(0, ImmutableArray.Create<TxInput>(), ImmutableArray.Create<TxOutput>(), 0));
            var emptyCoinbaseTx1 = BlockTx.Create(1, Transaction.Create(1, ImmutableArray.Create<TxInput>(), ImmutableArray.Create<TxOutput>(), 0));
            var emptyCoinbaseTx2 = BlockTx.Create(2, Transaction.Create(2, ImmutableArray.Create<TxInput>(), ImmutableArray.Create<TxOutput>(), 0));

            // initialize memory utxo builder storage
            var memoryStorage = new MemoryStorageManager();
            var memoryChainStateCursor = memoryStorage.OpenChainStateCursor().Item;
            memoryChainStateCursor.BeginTransaction();

            // initialize utxo builder
            var utxoBuilder = new UtxoBuilder();

            // prepare an unspent transaction
            var txHash = new UInt256(100);
            var unspentTx = new UnspentTx(txHash, chainedHeader1.Height, 0, 0, false, 3, OutputState.Unspent);
            var txOutput1Key = new TxOutputKey(txHash, 0);
            var txOutput1 = new TxOutput(0, ImmutableArray<byte>.Empty);
            var txOutput2Key = new TxOutputKey(txHash, 1);
            var txOutput2 = new TxOutput(1, ImmutableArray<byte>.Empty);
            var txOutput3Key = new TxOutputKey(txHash, 2);
            var txOutput3 = new TxOutput(2, ImmutableArray<byte>.Empty);

            // prepare unspent output
            var unspentTransactions = ImmutableDictionary.Create<UInt256, UnspentTx>().Add(txHash, unspentTx);

            // add the unspent transaction
            memoryChainStateCursor.TryAddUnspentTx(unspentTx);
            memoryChainStateCursor.TryAddUnspentTxOutput(txOutput1Key, txOutput1);
            memoryChainStateCursor.TryAddUnspentTxOutput(txOutput2Key, txOutput2);
            memoryChainStateCursor.TryAddUnspentTxOutput(txOutput3Key, txOutput3);

            // create an input to spend the unspent transaction's first output
            var input0 = new TxInput(txHash, 0, ImmutableArray.Create<byte>(), 0);
            var tx0 = BlockTx.Create(1, Transaction.Create(0, ImmutableArray.Create(input0), ImmutableArray.Create<TxOutput>(), 0));

            // spend the input
            chain.AddBlock(chainedHeader1);
            utxoBuilder.CalculateUtxo(memoryChainStateCursor, chain.ToImmutable(), new[] { emptyCoinbaseTx0, tx0 }.ToBufferBlock()).ToEnumerable().ToList();

            // verify utxo storage
            UnspentTx actualUnspentTx; TxOutput actualTxOutput;
            Assert.IsTrue(memoryChainStateCursor.TryGetUnspentTx(txHash, out actualUnspentTx));
            Assert.IsTrue(actualUnspentTx.OutputStates.Length == 3);
            Assert.IsTrue(actualUnspentTx.OutputStates[0] == OutputState.Spent);
            Assert.IsTrue(actualUnspentTx.OutputStates[1] == OutputState.Unspent);
            Assert.IsTrue(actualUnspentTx.OutputStates[2] == OutputState.Unspent);

            Assert.IsTrue(memoryChainStateCursor.TryGetUnspentTxOutput(txOutput1Key, out actualTxOutput));
            Assert.AreEqual(txOutput1, actualTxOutput);
            Assert.IsTrue(memoryChainStateCursor.TryGetUnspentTxOutput(txOutput2Key, out actualTxOutput));
            Assert.AreEqual(txOutput2, actualTxOutput);
            Assert.IsTrue(memoryChainStateCursor.TryGetUnspentTxOutput(txOutput3Key, out actualTxOutput));
            Assert.AreEqual(txOutput3, actualTxOutput);

            // create an input to spend the unspent transaction's second output
            var input1 = new TxInput(txHash, 1, ImmutableArray.Create<byte>(), 0);
            var tx1 = BlockTx.Create(1, Transaction.Create(0, ImmutableArray.Create(input1), ImmutableArray.Create<TxOutput>(), 0));

            // spend the input
            chain.AddBlock(chainedHeader2);
            utxoBuilder.CalculateUtxo(memoryChainStateCursor, chain.ToImmutable(), new[] { emptyCoinbaseTx1, tx1 }.ToBufferBlock()).ToEnumerable().ToList();

            // verify utxo storage
            Assert.IsTrue(memoryChainStateCursor.TryGetUnspentTx(txHash, out actualUnspentTx));
            Assert.IsTrue(actualUnspentTx.OutputStates.Length == 3);
            Assert.IsTrue(actualUnspentTx.OutputStates[0] == OutputState.Spent);
            Assert.IsTrue(actualUnspentTx.OutputStates[1] == OutputState.Spent);
            Assert.IsTrue(actualUnspentTx.OutputStates[2] == OutputState.Unspent);

            // create an input to spend the unspent transaction's third output
            var input2 = new TxInput(txHash, 2, ImmutableArray.Create<byte>(), 0);
            var tx2 = BlockTx.Create(2, Transaction.Create(0, ImmutableArray.Create(input2), ImmutableArray.Create<TxOutput>(), 0));

            // spend the input
            chain.AddBlock(chainedHeader3);
            utxoBuilder.CalculateUtxo(memoryChainStateCursor, chain.ToImmutable(), new[] { emptyCoinbaseTx2, tx2 }.ToBufferBlock()).ToEnumerable().ToList();

            // verify utxo storage
            Assert.IsTrue(memoryChainStateCursor.TryGetUnspentTx(txHash, out actualUnspentTx));
            Assert.IsTrue(actualUnspentTx.IsFullySpent);
        }

        [TestMethod]
        public void TestDoubleSpend()
        {
            // prepare block
            var fakeHeaders = new FakeHeaders();
            var chainedHeader0 = fakeHeaders.GenesisChained();
            var chainedHeader1 = fakeHeaders.NextChained();
            var chainedHeader2 = fakeHeaders.NextChained();
            var chain = Chain.CreateForGenesisBlock(chainedHeader0).ToBuilder();
            var emptyCoinbaseTx0 = BlockTx.Create(0, Transaction.Create(0, ImmutableArray.Create<TxInput>(), ImmutableArray.Create<TxOutput>(), 0));
            var emptyCoinbaseTx1 = BlockTx.Create(0, Transaction.Create(1, ImmutableArray.Create<TxInput>(), ImmutableArray.Create<TxOutput>(), 0));

            // initialize memory utxo builder storage
            var memoryStorage = new MemoryStorageManager();
            var memoryChainStateCursor = memoryStorage.OpenChainStateCursor().Item;
            memoryChainStateCursor.BeginTransaction();

            // initialize utxo builder
            var utxoBuilder = new UtxoBuilder();

            // prepare an unspent transaction
            var txHash = new UInt256(100);
            var unspentTx = new UnspentTx(txHash, chainedHeader1.Height, 0, 0, false, 1, OutputState.Unspent);
            var txOutputKey = new TxOutputKey(txHash, 0);
            var txOutput = new TxOutput(0, ImmutableArray<byte>.Empty);

            // add the unspent transaction
            memoryChainStateCursor.TryAddUnspentTx(unspentTx);
            memoryChainStateCursor.TryAddUnspentTxOutput(txOutputKey, txOutput);

            // create an input to spend the unspent transaction
            var input = new TxInput(txHash, 0, ImmutableArray.Create<byte>(), 0);
            var tx = BlockTx.Create(1, Transaction.Create(0, ImmutableArray.Create(input), ImmutableArray.Create<TxOutput>(), 0));

            // spend the input
            chain.AddBlock(chainedHeader1);
            utxoBuilder.CalculateUtxo(memoryChainStateCursor, chain.ToImmutable(), new[] { emptyCoinbaseTx0, tx }.ToBufferBlock()).ToEnumerable().ToList();

            // verify utxo storage
            UnspentTx actualUnspentTx; TxOutput actualTxOutput;
            Assert.IsTrue(memoryChainStateCursor.TryGetUnspentTx(txHash, out actualUnspentTx));
            Assert.IsTrue(actualUnspentTx.IsFullySpent);
            Assert.IsTrue(memoryChainStateCursor.TryGetUnspentTxOutput(txOutputKey, out actualTxOutput));
            Assert.AreEqual(txOutput, actualTxOutput);

            // attempt to spend the input again, validation exception should be thrown
            chain.AddBlock(chainedHeader2);
            AssertMethods.AssertAggregateThrows<ValidationException>(() =>
                utxoBuilder.CalculateUtxo(memoryChainStateCursor, chain.ToImmutable(), new[] { emptyCoinbaseTx1, tx }.ToBufferBlock()).ToEnumerable().ToList());
        }
    }
}
