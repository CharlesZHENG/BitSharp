using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Test;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NLog;
using System;

namespace BitSharp.IntegrationTest
{
    [Ignore]
    [TestClass]
    public class LargeBlockTest
    {
        [TestMethod]
        public void TestLargeBlocks()
        {
            using (var daemon = IntegrationTestDaemon.Create())
            {
                //daemon.CoreDaemon.PruningMode = PruningMode.TxIndex | PruningMode.BlockSpentIndex | PruningMode.BlockTxesPreserveMerkle;

                var logger = LogManager.GetCurrentClassLogger();
                var count = 1.THOUSAND();
                var txCount = 1.MILLION();

                var mineNextLock = new object();
                Action mineNextBlock = () =>
                {
                    lock (mineNextLock)
                    {
                        var height = daemon.TestBlocks.Chain.Height;
                        if (height + 1 >= count)
                            return;

                        logger.Info($"Mining block: {height + 1:N0}, daemon height: {daemon.CoreDaemon.CurrentChain.Height:N0}");
                        daemon.MineAndAddBlock(txCount);
                        logger.Info($"Added block:  {height + 1:N0}, daemon height: {daemon.CoreDaemon.CurrentChain.Height:N0}");
                    }
                };

                daemon.CoreDaemon.OnChainStateChanged += (_, __) => mineNextBlock();

                mineNextBlock();
                mineNextBlock();
                while (daemon.CoreDaemon.CurrentChain.Height + 1 < count)
                {
                    daemon.WaitForUpdate();
                }
            }
        }
    }
}
