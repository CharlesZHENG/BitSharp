using Ninject;
using Ninject.Modules;

namespace BitSharp.Core.Storage.Memory
{
    public class MemoryStorageModule : NinjectModule
    {
        public override void Load()
        {
            // bind concrete storage providers
            this.Bind<MemoryStorageManager>().ToSelf().InSingletonScope();

            // bind storage providers interfaces
            this.Bind<IStorageManager>().ToMethod(x => this.Kernel.Get<MemoryStorageManager>()).InSingletonScope();
        }
    }
}
