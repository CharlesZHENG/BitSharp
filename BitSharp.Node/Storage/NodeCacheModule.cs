using BitSharp.Node.Domain;
using Ninject;
using Ninject.Modules;
using Ninject.Parameters;

namespace BitSharp.Node.Storage
{
    public class NodeCacheModule : NinjectModule
    {
        private IBoundedCache<NetworkAddressKey, NetworkAddressWithTime> networkPeerCache;

        public override void Load()
        {
            var networkPeerStorage = this.Kernel.Get<INetworkPeerStorage>();
            this.networkPeerCache = this.Kernel.Get<BoundedCache<NetworkAddressKey, NetworkAddressWithTime>>(
                new ConstructorArgument("name", "Network Peer Cache"), new ConstructorArgument("dataStorage", networkPeerStorage));

            this.Bind<NetworkPeerCache>().ToSelf().InSingletonScope().WithConstructorArgument(this.networkPeerCache);
        }

        public override void Unload()
        {
            this.networkPeerCache.Dispose();

            base.Unload();
        }
    }
}
