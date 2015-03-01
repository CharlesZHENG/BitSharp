using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Test;
using BitSharp.Wallet.Address;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NLog;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Wallet.Test
{
    [TestClass]
    public class WalletMonitorTest
    {
        [TestMethod]
        [Timeout(300000/*ms*/)]
        public void TestMonitorAddress()
        {
            var publicKey =
                "04f9804cfb86fb17441a6562b07c4ee8f012bdb2da5be022032e4b87100350ccc7c0f4d47078b06c9d22b0ec10bdce4c590e0d01aed618987a6caa8c94d74ee6dc"
                .HexToByteArray().ToImmutableArray();

            using (var simulator = new MainnetSimulator())
            using (var walletMonitor = new WalletMonitor(simulator.CoreDaemon, LogManager.CreateNullLogger()))
            {
                walletMonitor.AddAddress(new PublicKeyAddress(publicKey));
                walletMonitor.Start();

                var block9999 = simulator.BlockProvider.GetBlock(9999);

                simulator.AddBlockRange(0, 9999);
                simulator.WaitForUpdate();
                AssertMethods.AssertDaemonAtBlock(9999, block9999.Hash, simulator.CoreDaemon);

                var minedTxOutputs = walletMonitor.Entries.Where(x => x.Type == EnumWalletEntryType.Mine).ToList();
                var receivedTxOutputs = walletMonitor.Entries.Where(x => x.Type == EnumWalletEntryType.Receive).ToList();
                var spentTxOutputs = walletMonitor.Entries.Where(x => x.Type == EnumWalletEntryType.Spend).ToList();

                var actualMinedBtc = minedTxOutputs.Sum(x => (decimal)x.Value) / 100.MILLION();
                var actualReceivedBtc = receivedTxOutputs.Sum(x => (decimal)x.Value) / 100.MILLION();
                var actualSpentBtc = spentTxOutputs.Sum(x => (decimal)x.Value) / 100.MILLION();

                Assert.AreEqual(0, minedTxOutputs.Count);
                Assert.AreEqual(16, receivedTxOutputs.Count);
                Assert.AreEqual(14, spentTxOutputs.Count);
                Assert.AreEqual(0M, actualMinedBtc);
                Assert.AreEqual(569.44M, actualReceivedBtc);
                Assert.AreEqual(536.52M, actualSpentBtc);
            }
        }
    }
}
