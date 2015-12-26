using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Script;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Linq;
using System.Text;

namespace BitSharp.Core.Test.Script
{
    [TestClass]
    public class TransactionManagerTest
    {
        //TODO this test occassionally generates an invalid public key and fails
        [TestMethod]
        public void TestCreateCoinbaseAndSpend()
        {
            var txManager = new TransactionManager();
            var keyPair = txManager.CreateKeyPair();
            var privateKey = keyPair.Item1;
            var publicKey = keyPair.Item2;

            var coinbaseTx = txManager.CreateCoinbaseTransaction(publicKey, Encoding.ASCII.GetBytes("coinbase text!")).Transaction;

            var publicKeyScript = txManager.CreatePublicKeyScript(publicKey);
            var privateKeyScript = txManager.CreatePrivateKeyScript(coinbaseTx, 0, (byte)ScriptHashType.SIGHASH_ALL, privateKey, publicKey);

            var script = privateKeyScript.Concat(publicKeyScript);

            var scriptEngine = new ScriptEngine();
            Assert.IsTrue(scriptEngine.VerifyScript(UInt256.Zero, 0, publicKeyScript.ToArray(), coinbaseTx, 0, script));
        }
    }
}
