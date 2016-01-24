using BitSharp.Network.Domain;
using System.Collections.Generic;

namespace BitSharp.Network.Storage
{
    public interface INetworkPeerStorage : IDictionary<NetworkAddressKey, NetworkAddressWithTime>
    {
    }
}
