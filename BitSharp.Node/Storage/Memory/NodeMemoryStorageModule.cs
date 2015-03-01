using Ninject;
using Ninject.Modules;

namespace BitSharp.Node.Storage.Memory
{
    public class NodeMemoryStorageModule : NinjectModule
    {
        public override void Load()
        {
            // bind concrete storage providers
            this.Bind<MemoryNetworkPeerStorage>().ToSelf().InSingletonScope();

            // bind storage providers interfaces
            this.Bind<INetworkPeerStorage>().ToMethod(x => this.Kernel.Get<MemoryNetworkPeerStorage>()).InSingletonScope();
        }
    }
}
