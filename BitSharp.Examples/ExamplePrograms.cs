using BitSharp.Common;
using BitSharp.Core;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using BitSharp.Core.Storage.Memory;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NLog;
using NLog.Config;
using NLog.Targets;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace BitSharp.Examples
{
    // methods are marked as tests to facilitate easy running of individual examples
    [TestClass]
    public class ExamplePrograms
    {
        public static void Main(string[] args)
        {
            new ExamplePrograms().RunAllExamples();

            if (Debugger.IsAttached)
            {
                Console.Write("Press any key to continue . . . ");
                Console.ReadKey();
            }
        }

        private readonly Logger logger;

        public ExamplePrograms()
        {
            var loggerNamePattern = "BitSharp.Examples.*";
            var logLevel = LogLevel.Info;

            // log layout format
            var layout = "${message} ${exception:separator=\r\n:format=message,type,method,stackTrace:maxInnerExceptionLevel=10:innerExceptionSeparator=\r\n:innerFormat=message,type,method,stackTrace}";

            // initialize logging configuration
            var config = new LoggingConfiguration();

            // create console target
            var consoleTarget = new ColoredConsoleTarget();
            consoleTarget.Layout = layout;
            config.AddTarget("console", consoleTarget);
            config.LoggingRules.Add(new LoggingRule(loggerNamePattern, logLevel, consoleTarget));

            // create debugger target, if attached
            if (Debugger.IsAttached)
            {
                var debuggerTarget = new DebuggerTarget();
                debuggerTarget.Layout = layout;
                config.AddTarget("debugger", debuggerTarget);
                config.LoggingRules.Add(new LoggingRule(loggerNamePattern, logLevel, debuggerTarget));
            }

            LogManager.Configuration = config;
            logger = LogManager.GetCurrentClassLogger();
        }

        private void RunAllExamples()
        {
            foreach (var exampleMethod in GetType().GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.DeclaredOnly))
            {
                logger.Info(string.Format("Running example: {0}", exampleMethod.Name));
                exampleMethod.Invoke(this, new object[0]);
                logger.Info(string.Format("Finished running example: {0}", exampleMethod.Name));
                logger.Info("-------------");
                logger.Info("");
            }

            logger.Info("Finished running examples");
            logger.Info("");
        }

        [TestMethod]
        public void ExampleDaemon()
        {
            // create example core daemon
            BlockProvider embeddedBlocks; IStorageManager storageManager;
            using (var coreDaemon = CreateExampleDaemon(out embeddedBlocks, out storageManager, maxHeight: 99))
            using (embeddedBlocks)
            using (storageManager)
            {
                // report core daemon's progress
                logger.Info(string.Format("Core daemon height: {0:N0}", coreDaemon.CurrentChain.Height));
            }
        }

        private CoreDaemon CreateExampleDaemon(out BlockProvider embeddedBlocks, out IStorageManager storageManager, int? maxHeight = null)
        {
            // retrieve first 10,000 testnet3 blocks
            embeddedBlocks = new BlockProvider("BitSharp.Examples.Blocks.TestNet3.zip");

            // initialize in-memory storage
            storageManager = new MemoryStorageManager();

            // intialize testnet3 rules (ignore script errors, script engine is not and is not intended to be complete)
            var rules = new Testnet3Rules { IgnoreScriptErrors = true };

            // initialize & start core daemon
            var coreDaemon = new CoreDaemon(rules, storageManager) { MaxHeight = maxHeight, IsStarted = true };

            // add embedded blocks
            coreDaemon.CoreStorage.AddBlocks(embeddedBlocks.ReadBlocks());

            // wait for core daemon to finish processing any available data
            coreDaemon.WaitForUpdate();

            return coreDaemon;
        }

        [TestMethod]
        public void ChainStateExample()
        {
            // create example core daemon
            BlockProvider embeddedBlocks; IStorageManager storageManager;
            using (var coreDaemon = CreateExampleDaemon(out embeddedBlocks, out storageManager, maxHeight: 999))
            using (embeddedBlocks)
            using (storageManager)
            // retrieve an immutable snapshot of the current chainstate, validation won't be blocked by an open snapshot
            using (var chainState = coreDaemon.GetChainState())
            {
                // retrieve unspent transactions
                var unspentTxes = chainState.ReadUnspentTransactions().ToList();

                // report counts
                logger.Info(string.Format("Chain.Height:                      {0,9:N0}", chainState.Chain.Height));
                logger.Info(string.Format("ReadUnspentTransactions().Count(): {0,9:N0}", unspentTxes.Count));
                logger.Info(string.Format("UnspentTxCount:                    {0,9:N0}", chainState.UnspentTxCount));
                logger.Info(string.Format("UnspentOutputCount:                {0,9:N0}", chainState.UnspentOutputCount));
                logger.Info(string.Format("TotalTxCount:                      {0,9:N0}", chainState.TotalTxCount));
                logger.Info(string.Format("TotalInputCount:                   {0,9:N0}", chainState.TotalInputCount));
                logger.Info(string.Format("TotalOutputCount:                  {0,9:N0}", chainState.TotalOutputCount));

                // look up genesis coinbase output (will be missing)
                UnspentTx unspentTx;
                chainState.TryGetUnspentTx(embeddedBlocks.GetBlock(0).Transactions[0].Hash, out unspentTx);
                logger.Info(string.Format("Gensis coinbase UnspentTx present? {0,9}", unspentTx != null));

                // look up block 1 coinbase output
                chainState.TryGetUnspentTx(embeddedBlocks.GetBlock(1).Transactions[0].Hash, out unspentTx);
                logger.Info(string.Format("Block 1 coinbase UnspenTx present? {0,9}", unspentTx != null));
                logger.Info(string.Format("Block 1 coinbase output states:    [{0}]", string.Join(",", unspentTx.OutputStates.Select(x => x.ToString()))));

                // look up block 381 list of spent txes
                IImmutableList<UInt256> spentTxes;
                chainState.TryGetBlockSpentTxes(381, out spentTxes);
                logger.Info(string.Format("Block 381 spent txes count:        {0,9:N0}", spentTxes.Count));
            }
        }
    }
}
