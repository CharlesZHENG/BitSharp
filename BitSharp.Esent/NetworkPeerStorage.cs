using BitSharp.Core.Rules;
using BitSharp.Node;
using BitSharp.Node.Domain;
using BitSharp.Node.Storage;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace BitSharp.Esent
{
    public class NetworkPeerStorage : PersistentObjectDictonary<NetworkAddressKey, NetworkAddressWithTime>, INetworkPeerStorage
    {
        public NetworkPeerStorage(string baseDirectory, RulesEnum rulesType)
            : base(Path.Combine(baseDirectory, "KnownAddresses"),
                keyEncoder: key => NodeEncoder.EncodeNetworkAddressKey(key),
                keyDecoder: key => NodeEncoder.DecodeNetworkAddressKey(key),
                valueEncoder: value => NodeEncoder.EncodeNetworkAddressWithTime(value),
                valueDecoder: value => NodeEncoder.DecodeNetworkAddressWithTime(value))
        {
        }
    }
}
