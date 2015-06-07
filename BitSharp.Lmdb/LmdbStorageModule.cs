using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using BitSharp.Node.Storage;
using Ninject;
using Ninject.Modules;
using System.IO;

namespace BitSharp.Lmdb
{
    public class LmdbStorageModule : NinjectModule
    {
        private readonly string baseDirectory;
        private readonly string dataDirectory;
        private readonly RulesEnum rulesType;

        public LmdbStorageModule(string baseDirectory, RulesEnum rulesType)
        {
            this.baseDirectory = baseDirectory;
            this.dataDirectory = Path.Combine(baseDirectory, "Data", rulesType.ToString());
            this.rulesType = rulesType;
        }

        public override void Load()
        {
            // bind concrete storage providers
            this.Bind<LmdbStorageManager>().ToSelf().InSingletonScope().WithConstructorArgument("baseDirectory", this.dataDirectory);

            // bind storage providers interfaces
            this.Bind<IStorageManager>().ToMethod(x => this.Kernel.Get<LmdbStorageManager>()).InSingletonScope();
        }
    }
}
