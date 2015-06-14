using BitSharp.Node.Domain;
using System.Collections.Generic;

namespace BitSharp.Node.Storage
{
    public interface INetworkPeerStorage : IDictionary<NetworkAddressKey, NetworkAddressWithTime>
    {
    }
}
