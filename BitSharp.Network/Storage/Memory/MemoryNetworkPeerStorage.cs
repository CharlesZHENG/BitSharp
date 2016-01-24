using BitSharp.Network.Domain;
using System.Collections.Concurrent;

namespace BitSharp.Network.Storage.Memory
{
    public sealed class MemoryNetworkPeerStorage : ConcurrentDictionary<NetworkAddressKey, NetworkAddressWithTime>, INetworkPeerStorage
    {
    }
}
