using BitSharp.Core.Rules;
using BitSharp.Node;
using BitSharp.Node.Domain;
using BitSharp.Node.Storage;
using System.IO;

namespace BitSharp.LevelDb
{
    public class NetworkPeerStorage : PersistentObjectDictonary<NetworkAddressKey, NetworkAddressWithTime>, INetworkPeerStorage
    {
        public NetworkPeerStorage(string baseDirectory, ChainType chainType)
            : base(Path.Combine(baseDirectory, "KnownAddresses"),
                keyEncoder: key => NodeEncoder.EncodeNetworkAddressKey(key),
                keyDecoder: key => NodeEncoder.DecodeNetworkAddressKey(key),
                valueEncoder: value => NodeEncoder.EncodeNetworkAddressWithTime(value),
                valueDecoder: value => NodeEncoder.DecodeNetworkAddressWithTime(value))
        {
        }
    }
}
