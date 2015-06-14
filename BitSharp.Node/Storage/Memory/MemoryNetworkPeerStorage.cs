using BitSharp.Node.Domain;
using System.Collections.Concurrent;

namespace BitSharp.Node.Storage.Memory
{
    public sealed class MemoryNetworkPeerStorage : ConcurrentDictionary<NetworkAddressKey, NetworkAddressWithTime>, INetworkPeerStorage
    {
    }
}
