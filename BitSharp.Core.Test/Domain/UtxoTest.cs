﻿using BitSharp.Common;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage.Memory;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BitSharp.Core.Test.Domain
{
    //TODO turn this into a proper IChainStateTest
    [TestClass]
    public class UtxoTest
    {
        [TestMethod]
        public void TestCanSpend_Unspent()
        {
            // prepare utxo storage
            var chain = new Chain(ImmutableList.Create(new FakeHeaders().GenesisChained()));
            var unspentTransactions = ImmutableSortedDictionary.CreateBuilder<UInt256, UnspentTx>();

            // prepare unspent output
            var txHash = new UInt256(0);
            unspentTransactions.Add(txHash, new UnspentTx(txHash, blockIndex: 0, txIndex: 0, length: 1, state: OutputState.Unspent));

            // prepare utxo
            var memoryStorage = new MemoryStorageManager(unspentTransactions: unspentTransactions.ToImmutable());
            var chainStateStorage = memoryStorage.OpenChainStateCursor().Item;
            chainStateStorage.AddChainedHeader(chain.GenesisBlock);
            var utxo = new ChainState(chain, memoryStorage);

            // prepare output reference
            var prevTxOutput = new TxOutputKey(txHash, txOutputIndex: 0);

            // check if output can be spent
            var canSpend = utxo.CanSpend(prevTxOutput);

            // verify output can be spent
            Assert.IsTrue(canSpend);
        }

        [TestMethod]
        public void TestCanSpend_Spent()
        {
            // prepare utxo storage
            var chain = new Chain(ImmutableList.Create(new FakeHeaders().GenesisChained()));
            var unspentTransactions = ImmutableSortedDictionary.CreateBuilder<UInt256, UnspentTx>();

            // prepare spent output
            var txHash = new UInt256(0);
            unspentTransactions.Add(txHash, new UnspentTx(txHash, blockIndex: 0, txIndex: 0, length: 1, state: OutputState.Spent));

            // prepare utxo
            var memoryStorage = new MemoryStorageManager(unspentTransactions: unspentTransactions.ToImmutable());
            var chainStateStorage = memoryStorage.OpenChainStateCursor().Item;
            chainStateStorage.AddChainedHeader(chain.GenesisBlock);
            var utxo = new ChainState(chain, memoryStorage);

            // prepare output reference
            var prevTxOutput = new TxOutputKey(txHash, txOutputIndex: 0);

            // check if output can be spent
            var canSpend = utxo.CanSpend(prevTxOutput);

            // verify output cannot be spent
            Assert.IsFalse(canSpend);
        }

        [TestMethod]
        public void TestCanSpend_Missing()
        {
            // prepare utxo
            var chain = new Chain(ImmutableList.Create(new FakeHeaders().GenesisChained()));
            var memoryStorage = new MemoryStorageManager();
            var chainStateStorage = memoryStorage.OpenChainStateCursor().Item;
            chainStateStorage.AddChainedHeader(chain.GenesisBlock);
            var utxo = new ChainState(chain, memoryStorage);

            // prepare output reference
            var prevTxOutput = new TxOutputKey(txHash: 0, txOutputIndex: 0);

            // check if output can be spent
            var canSpend = utxo.CanSpend(prevTxOutput);

            // verify output cannot be spent
            Assert.IsFalse(canSpend);
        }

        [TestMethod]
        public void TestCanSpend_NegativeIndex()
        {
            // prepare utxo storage
            var chain = new Chain(ImmutableList.Create(new FakeHeaders().GenesisChained()));
            var unspentTransactions = ImmutableSortedDictionary.CreateBuilder<UInt256, UnspentTx>();

            // prepare unspent output
            var txHash = new UInt256(0);
            unspentTransactions.Add(txHash, new UnspentTx(txHash, blockIndex: 0, txIndex: 0, length: 1, state: OutputState.Unspent));

            // prepare utxo
            var memoryStorage = new MemoryStorageManager(unspentTransactions: unspentTransactions.ToImmutable());
            var chainStateStorage = memoryStorage.OpenChainStateCursor().Item;
            chainStateStorage.AddChainedHeader(chain.GenesisBlock);
            var utxo = new ChainState(chain, memoryStorage);

            // prepare output reference
            var prevTxOutput = new TxOutputKey(txHash, txOutputIndex: UInt32.MaxValue);

            // check if output can be spent
            var canSpend = utxo.CanSpend(prevTxOutput);

            // verify output cannot be spent
            Assert.IsFalse(canSpend);
        }

        [TestMethod]
        public void TestCanSpend_IndexOutOfRange()
        {
            // prepare utxo storage
            var chain = new Chain(ImmutableList.Create(new FakeHeaders().GenesisChained()));
            var unspentTransactions = ImmutableSortedDictionary.CreateBuilder<UInt256, UnspentTx>();

            // prepare unspent output
            var txHash = new UInt256(0);
            unspentTransactions.Add(txHash, new UnspentTx(txHash, blockIndex: 0, txIndex: 0, length: 1, state: OutputState.Unspent));

            // prepare utxo
            var memoryStorage = new MemoryStorageManager(unspentTransactions: unspentTransactions.ToImmutable());
            var chainStateStorage = memoryStorage.OpenChainStateCursor().Item;
            chainStateStorage.AddChainedHeader(chain.GenesisBlock);
            var utxo = new ChainState(chain, memoryStorage);

            // prepare output reference
            var prevTxOutput = new TxOutputKey(txHash, txOutputIndex: 1);

            // check if output can be spent
            var canSpend = utxo.CanSpend(prevTxOutput);

            // verify output cannot be spent
            Assert.IsFalse(canSpend);
        }
    }
}
