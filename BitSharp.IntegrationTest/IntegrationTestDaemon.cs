using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Esent;
using BitSharp.Node;
using Ninject.Modules;
using NLog;

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

        public static IntegrationTestDaemon Create(Block genesisBlock = null)
        {
            var baseDirectory = TempDirectory.CreateTempDirectory();

            var loggingModule = new LoggingModule(baseDirectory, LogLevel.Info);

            var storageModules =
                new[] { new EsentStorageModule(baseDirectory, ChainType.Regtest, cacheSizeMaxBytes: 500.MILLION()) };

            return new IntegrationTestDaemon(genesisBlock, baseDirectory, loggingModule, storageModules);
        }
    }
}
