using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Script;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NLog;
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
            var txManager = new TransactionManager(LogManager.CreateNullLogger());
            var keyPair = txManager.CreateKeyPair();
            var privateKey = keyPair.Item1;
            var publicKey = keyPair.Item2;

            var coinbaseTx = txManager.CreateCoinbaseTransaction(publicKey, Encoding.ASCII.GetBytes("coinbase text!"));

            var publicKeyScript = txManager.CreatePublicKeyScript(publicKey);
            var privateKeyScript = txManager.CreatePrivateKeyScript(coinbaseTx, 0, (byte)ScriptHashType.SIGHASH_ALL, privateKey, publicKey);

            var script = privateKeyScript.Concat(publicKeyScript);

            var scriptEngine = new ScriptEngine(LogManager.CreateNullLogger());
            Assert.IsTrue(scriptEngine.VerifyScript(0, 0, publicKeyScript.ToArray(), coinbaseTx, 0, script));
        }
    }
}
