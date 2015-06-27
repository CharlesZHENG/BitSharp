using BitSharp.Core;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using BitSharp.Core.Storage.Memory;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
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

        private void RunAllExamples()
        {
            foreach (var exampleMethod in GetType().GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.DeclaredOnly))
            {
                Console.WriteLine(string.Format("Running example: {0}", exampleMethod.Name));
                exampleMethod.Invoke(this, new object[0]);
                Console.WriteLine(string.Format("Finished running example: {0}", exampleMethod.Name));
                Console.WriteLine("-------------");
                Console.WriteLine();
            }

            Console.WriteLine("Finished running examples");
            Console.WriteLine();
        }

        [TestMethod]
        public void ExampleDaemon()
        {
            // create example core daemon
            BlockProvider embeddedBlocks; IStorageManager storageManager;
            using (var coreDaemon = CreateExampleDaemon(out embeddedBlocks, out storageManager))
            using (embeddedBlocks)
            using (storageManager)
            {
                // report core daemon's progress
                Console.WriteLine(string.Format("Core daemon height: {0:N0}", coreDaemon.CurrentChain.Height));
            }
        }

        private CoreDaemon CreateExampleDaemon(out BlockProvider embeddedBlocks, out IStorageManager storageManager)
        {
            // retrieve first 100 mainnet blocks
            embeddedBlocks = new BlockProvider("BitSharp.Examples.Blocks.Mainnet.zip");

            // initialize in-memory storage
            storageManager = new MemoryStorageManager();

            // initialize & start core daemon, with mainnet rules
            var coreDaemon = new CoreDaemon(new MainnetRules(), storageManager) { IsStarted = true };

            // add embedded blocks
            coreDaemon.CoreStorage.AddBlocks(embeddedBlocks.ReadBlocks());

            // wait for core daemon to finish processing any available data
            coreDaemon.WaitForUpdate();

            return coreDaemon;
        }
    }
}
