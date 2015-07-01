using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Script;
using BitSharp.Core.Storage;
using BitSharp.Core.Storage.Memory;
using BitSharp.Core.Test.Rules;
using BitSharp.Esent;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Ninject;
using NLog;
using Org.BouncyCastle.Crypto.Parameters;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using BitSharp.IntegrationTest;
using BitSharp.Node;
using BitSharp.Lmdb;
using Ninject.Modules;

namespace BitSharp.Core.Test
{
    public class IntegrationTestDaemon : TestDaemon
    {
        private readonly string baseDirectory;
        private bool isDisposed;

        private IntegrationTestDaemon(Block genesisBlock, string baseDirectory, INinjectModule loggingModule, INinjectModule[] storageModules)
            : base(genesisBlock, loggingModule, storageModules)
        {
            this.baseDirectory = baseDirectory;
        }

        protected override void Dispose(bool disposing)
        {
            if (!isDisposed && disposing)
            {
                TempDirectory.DeleteDirectory(baseDirectory);
                isDisposed = true;
            }
        }

        public static IntegrationTestDaemon Create(Block genesisBlock = null, bool useLmdb = false)
        {
            var baseDirectory = TempDirectory.CreateTempDirectory();

            var loggingModule = new LoggingModule(baseDirectory, LogLevel.Info);

            var storageModules = useLmdb ?
                new INinjectModule[]
                {
                    new EsentStorageModule(baseDirectory, RulesEnum.TestNet2, blockStorage: !useLmdb, cacheSizeMaxBytes: 500.MILLION()),
                    new LmdbStorageModule(baseDirectory, RulesEnum.TestNet2)
                }
                :
                new[] { new EsentStorageModule(baseDirectory, RulesEnum.TestNet2, cacheSizeMaxBytes: 500.MILLION()) };

            return new IntegrationTestDaemon(genesisBlock, baseDirectory, loggingModule, storageModules);
        }
    }
}
