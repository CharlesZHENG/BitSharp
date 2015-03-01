using BitSharp.Node.Domain;

namespace BitSharp.Node.Storage
{
    public interface INetworkPeerStorage :
        IBoundedStorage<NetworkAddressKey, NetworkAddressWithTime> { }

    public sealed class NetworkPeerCache : PassthroughBoundedCache<NetworkAddressKey, NetworkAddressWithTime>
    {
        public NetworkPeerCache(IBoundedCache<NetworkAddressKey, NetworkAddressWithTime> cache)
            : base(cache) { }
    }
}
