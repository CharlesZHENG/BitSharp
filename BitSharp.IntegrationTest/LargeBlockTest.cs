using BitSharp.Common.ExtensionMethods;
using BitSharp.Core;
using BitSharp.Core.Domain;
using BitSharp.Core.Test;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BitSharp.IntegrationTest
{
    [Ignore]
    [TestClass]
    public class LargeBlockTest
    {
        [TestMethod]
        public void TestLargeBlocks()
        {
            using (var daemon = new IntegrationTestDaemon(useLmdb: false))
            {
                //daemon.CoreDaemon.PruningMode = PruningMode.TxIndex | PruningMode.BlockSpentIndex | PruningMode.BlockTxesPreserveMerkle;

                var logger = LogManager.GetCurrentClassLogger();
                var count = 1.THOUSAND();
                var txCount = 1.MILLION();

                var block = daemon.GenesisBlock;

                var mineNextLock = new object();
                Action mineNextBlock = () =>
                {
                    lock (mineNextLock)
                    {
                        var height = daemon.CoreStorage.GetChainedHeader(block.Hash).Height;
                        if (height + 1 >= count)
                            return;

                        logger.Info("Mining block: {0:N0}, daemon height: {1:N0}".Format2(height + 1, daemon.CoreDaemon.CurrentChain.Height));
                        block = daemon.MineAndAddLargeBlock(block, txCount);
                        logger.Info("Added block:  {0:N0}, daemon height: {1:N0}".Format2(height + 1, daemon.CoreDaemon.CurrentChain.Height));
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
