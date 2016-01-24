using BitSharp.Core.Rules;
using BitSharp.Network;
using BitSharp.Network.Domain;
using BitSharp.Network.Storage;
using System.IO;

namespace BitSharp.Esent
{
    public class NetworkPeerStorage : PersistentObjectDictonary<NetworkAddressKey, NetworkAddressWithTime>, INetworkPeerStorage
    {
        public NetworkPeerStorage(string baseDirectory, ChainType chainType)
            : base(Path.Combine(baseDirectory, "KnownAddresses"),
                keyEncoder: key => NetworkEncoder.EncodeNetworkAddressKey(key),
                keyDecoder: key => NetworkEncoder.DecodeNetworkAddressKey(key),
                valueEncoder: value => NetworkEncoder.EncodeNetworkAddressWithTime(value),
                valueDecoder: value => NetworkEncoder.DecodeNetworkAddressWithTime(value))
        {
        }
    }
}
