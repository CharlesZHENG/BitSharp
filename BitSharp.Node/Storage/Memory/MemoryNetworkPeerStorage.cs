using BitSharp.Node.Domain;

namespace BitSharp.Node.Storage.Memory
{
    public sealed class MemoryNetworkPeerStorage : MemoryStorage<NetworkAddressKey, NetworkAddressWithTime>, INetworkPeerStorage { }
}
