﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using BitSharp;
using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using System.Collections.Immutable;
using NLog;
using Ninject;
using BitSharp.Core.Domain;
using BitSharp.Core.Script;

namespace BitSharp.Core.Test.Script
{
    [TestClass]
    public class ScriptEngineTest
    {
        private static MainnetBlockProvider provider;

        private IKernel kernel;
        private Logger logger;

        [ClassInitialize]
        public static void ClassInitialize(TestContext context)
        {
            ScriptEngineTest.provider = new MainnetBlockProvider();
        }

        [TestInitialize]
        public void TestInitialize()
        {
            this.kernel = new StandardKernel(new ConsoleLoggingModule());
            this.logger = this.kernel.Get<Logger>();
        }

        [TestCleanup]
        public void TestCleanup()
        {
        }

        [TestMethod]
        public void TestFirstTransactionSignature()
        {
            Block block; Transaction tx; IDictionary<UInt256, Transaction> txLookup;
            MainnetTestData.GetFirstTransaction(provider, out block, out tx, out txLookup);

            TestTransactionSignature(MainnetTestData.TX_0_SIGNATURES, tx, txLookup);
        }

        [TestMethod]
        public void TestFirstMultiInputTransactionSignature()
        {
            Block block; Transaction tx; IDictionary<UInt256, Transaction> txLookup;
            MainnetTestData.GetFirstMultiInputTransaction(provider, out block, out tx, out txLookup);

            TestTransactionSignature(MainnetTestData.TX_0_MULTI_INPUT_SIGNATURES, tx, txLookup);
        }

        [TestMethod]
        public void TestFirstHash160TransactionSignature()
        {
            Block block; Transaction tx; IDictionary<UInt256, Transaction> txLookup;
            MainnetTestData.GetFirstHash160Transaction(provider, out block, out tx, out txLookup);

            TestTransactionSignature(MainnetTestData.TX_0_HASH160_SIGNATURES, tx, txLookup);
        }

        [TestMethod]
        public void TestFirstTransactionVerifySignature()
        {
            Block block; Transaction tx; IDictionary<UInt256, Transaction> txLookup;
            MainnetTestData.GetFirstTransaction(provider, out block, out tx, out txLookup);

            TestTransactionVerifySignature(MainnetTestData.TX_0_HASH_TYPES, MainnetTestData.TX_0_SIGNATURES, MainnetTestData.TX_0_SIGNATURE_HASHES, tx, txLookup);
        }

        [TestMethod]
        public void TestFirstMultiInputTransactionVerifySignature()
        {
            Block block; Transaction tx; IDictionary<UInt256, Transaction> txLookup;
            MainnetTestData.GetFirstMultiInputTransaction(provider, out block, out tx, out txLookup);

            TestTransactionVerifySignature(MainnetTestData.TX_0_MULTI_INPUT_HASH_TYPES, MainnetTestData.TX_0_MULTI_INPUT_SIGNATURES, MainnetTestData.TX_0_MULTI_INPUT_SIGNATURE_HASHES, tx, txLookup);
        }

        [TestMethod]
        public void TestFirstHash160TransactionVerifySignature()
        {
            Block block; Transaction tx; IDictionary<UInt256, Transaction> txLookup;
            MainnetTestData.GetFirstHash160Transaction(provider, out block, out tx, out txLookup);

            TestTransactionVerifySignature(MainnetTestData.TX_0_HASH160_HASH_TYPES, MainnetTestData.TX_0_HASH160_SIGNATURES, MainnetTestData.TX_0_HASH160_SIGNATURE_HASHES, tx, txLookup);
        }

        [TestMethod]
        public void TestFirstTransactionVerifyScript()
        {
            Block block; Transaction tx; IDictionary<UInt256, Transaction> txLookup;
            MainnetTestData.GetFirstTransaction(provider, out block, out tx, out txLookup);

            TestTransactionVerifyScript(tx, txLookup);
        }

        [TestMethod]
        public void TestFirstMultiInputTransactionVerifyScript()
        {
            Block block; Transaction tx; IDictionary<UInt256, Transaction> txLookup;
            MainnetTestData.GetFirstMultiInputTransaction(provider, out block, out tx, out txLookup);

            TestTransactionVerifyScript(tx, txLookup);
        }

        [TestMethod]
        public void TestFirstHash160TransactionVerifyScript()
        {
            Block block; Transaction tx; IDictionary<UInt256, Transaction> txLookup;
            MainnetTestData.GetFirstHash160Transaction(provider, out block, out tx, out txLookup);

            TestTransactionVerifyScript(tx, txLookup);
        }

        private void TestTransactionSignature(byte[][] expectedSignatures, Transaction tx, IDictionary<UInt256, Transaction> txLookup)
        {
            var scriptEngine = new ScriptEngine(this.logger);
            for (var inputIndex = 0; inputIndex < tx.Inputs.Length; inputIndex++)
            {
                var input = tx.Inputs[inputIndex];
                var prevOutput = txLookup[input.PreviousTxOutputKey.TxHash].Outputs[input.PreviousTxOutputKey.TxOutputIndex.ToIntChecked()];

                var hashType = GetHashTypeFromScriptSig(input.ScriptSignature);

                var actual = scriptEngine.TxSignature(prevOutput.ScriptPublicKey, tx, inputIndex, hashType);
                CollectionAssert.AreEqual(expectedSignatures[inputIndex].ToList(), actual.ToList());
            }
        }

        private void TestTransactionVerifySignature(byte[] expectedHashTypes, byte[][] expectedSignatures, byte[][] expectedSignatureHashes, Transaction tx, IDictionary<UInt256, Transaction> txLookup)
        {
            var scriptEngine = new ScriptEngine(this.logger);

            for (var inputIndex = 0; inputIndex < tx.Inputs.Length; inputIndex++)
            {
                var input = tx.Inputs[inputIndex];
                var prevOutput = txLookup[input.PreviousTxOutputKey.TxHash].Outputs[input.PreviousTxOutputKey.TxOutputIndex.ToIntChecked()];

                var hashType = GetHashTypeFromScriptSig(input.ScriptSignature);
                var sig = GetSigFromScriptSig(input.ScriptSignature);
                var pubKey = GetPubKeyFromScripts(input.ScriptSignature, prevOutput.ScriptPublicKey);

                byte[] txSignature, txSignatureHash;
                var result = scriptEngine.VerifySignature(prevOutput.ScriptPublicKey, tx, sig.ToArray(), pubKey.ToArray(), inputIndex, out hashType, out txSignature, out txSignatureHash);

                Assert.AreEqual(expectedHashTypes[inputIndex], hashType);
                CollectionAssert.AreEqual(expectedSignatures[inputIndex].ToList(), txSignature.ToList());
                CollectionAssert.AreEqual(expectedSignatureHashes[inputIndex].ToList(), txSignatureHash.ToList());
                Assert.IsTrue(result);
            }
        }

        private void TestTransactionVerifyScript(Transaction tx, IDictionary<UInt256, Transaction> txLookup)
        {
            var scriptEngine = new ScriptEngine(this.logger);

            for (var inputIndex = 0; inputIndex < tx.Inputs.Length; inputIndex++)
            {
                var input = tx.Inputs[inputIndex];
                var prevOutput = txLookup[input.PreviousTxOutputKey.TxHash].Outputs[input.PreviousTxOutputKey.TxOutputIndex.ToIntChecked()];

                var script = GetScriptFromInputPrevOutput(input, prevOutput);

                var result = scriptEngine.VerifyScript(0 /*blockIndex*/, -1 /*txIndex*/, prevOutput.ScriptPublicKey.ToArray(), tx, inputIndex, script.ToArray());

                Assert.IsTrue(result);
            }
        }

        private static ImmutableArray<byte> GetSigFromScriptSig(ImmutableArray<byte> scriptSig)
        {
            Debug.Assert(scriptSig[0] >= (int)ScriptOp.OP_PUSHBYTES1 && scriptSig[0] <= (int)ScriptOp.OP_PUSHBYTES75);
            // The first byte of scriptSig will be OP_PUSHBYTES, so the first byte indicates how many bytes to take to get sig from scriptSig
            return scriptSig.Skip(1).Take(scriptSig[0]).ToImmutableArray();
        }

        private static byte GetHashTypeFromScriptSig(ImmutableArray<byte> scriptSig)
        {
            return GetSigFromScriptSig(scriptSig).Last();
        }

        private static ImmutableArray<byte> GetPubKeyFromScripts(ImmutableArray<byte> scriptSig, ImmutableArray<byte> pubKey)
        {
            if (scriptSig.Length > scriptSig[0] + 1)
            {
                var result = scriptSig.Skip(1 + scriptSig[0] + 1).Take(scriptSig.Skip(1 + scriptSig[0]).First()).ToImmutableArray();
                return result;
            }
            else
            {
                return pubKey.Skip(1).Take(pubKey.Length - 2).ToImmutableArray();
            }
        }

        private static ImmutableArray<byte> GetScriptFromInputPrevOutput(TxInput input, TxOutput prevOutput)
        {
            return input.ScriptSignature.AddRange(prevOutput.ScriptPublicKey);
        }
    }
}
