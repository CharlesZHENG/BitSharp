using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using Ninject;
using Ninject.Modules;
using System.IO;

namespace BitSharp.Lmdb
{
    public class LmdbStorageModule : NinjectModule
    {
        private readonly string baseDirectory;
        private readonly string[] blockTxesStorageLocations;
        private readonly string dataDirectory;
        private readonly ChainTypeEnum rulesType;

        public LmdbStorageModule(string baseDirectory, ChainTypeEnum rulesType, string[] blockTxesStorageLocations = null)
        {
            this.baseDirectory = baseDirectory;
            this.blockTxesStorageLocations = blockTxesStorageLocations;
            this.dataDirectory = Path.Combine(baseDirectory, "Data", rulesType.ToString());
            this.rulesType = rulesType;
        }

        public override void Load()
        {
            // bind concrete storage providers
            this.Bind<LmdbStorageManager>().ToSelf().InSingletonScope()
                .WithConstructorArgument("baseDirectory", this.dataDirectory)
                .WithConstructorArgument("blockTxesStorageLocations", this.blockTxesStorageLocations);

            // bind storage providers interfaces
            this.Bind<IStorageManager>().ToMethod(x => this.Kernel.Get<LmdbStorageManager>()).InSingletonScope();
        }
    }
}
