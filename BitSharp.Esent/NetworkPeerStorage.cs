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
    public class NetworkPeerStorage : INetworkPeerStorage
    {
        private readonly string name;
        private readonly string directory;
        private readonly PersistentByteDictionary dict;

        public NetworkPeerStorage(string baseDirectory, RulesEnum rulesType)
        {
            this.name = "KnownAddresses";
            this.directory = Path.Combine(baseDirectory, this.name);
            this.dict = new PersistentByteDictionary(this.directory);
        }

        public void Dispose()
        {
            this.dict.Dispose();
        }

        public int Count
        {
            get { return this.dict.Count; }
        }

        public IEnumerable<NetworkAddressKey> Keys
        {
            get { return this.Select(x => x.Key); }
        }

        public IEnumerable<NetworkAddressWithTime> Values
        {
            get { return this.Select(x => x.Value); }
        }

        public bool ContainsKey(NetworkAddressKey networkAddressKey)
        {
            return this.dict.ContainsKey(EncodeKey(networkAddressKey));
        }

        public bool TryGetValue(NetworkAddressKey networkAddressKey, out NetworkAddressWithTime networkAddressWithTime)
        {
            byte[] networkAddressWithTimeBytes;
            if (this.dict.TryGetValue(EncodeKey(networkAddressKey), out networkAddressWithTimeBytes))
            {
                networkAddressWithTime = DecodeValue(networkAddressWithTimeBytes);
                return true;
            }
            else
            {
                networkAddressWithTime = default(NetworkAddressWithTime);
                return false;
            }
        }

        public bool TryAdd(NetworkAddressKey networkAddressKey, NetworkAddressWithTime networkAddressWithTime)
        {
            if (!this.ContainsKey(networkAddressKey))
            {
                try
                {
                    this.dict.Add(EncodeKey(networkAddressKey), EncodeValue(networkAddressWithTime));
                    return true;
                }
                catch (ArgumentException)
                { return false; }
            }
            else
            {
                return false;
            }
        }

        public bool TryRemove(NetworkAddressKey networkAddressKey)
        {
            return this.dict.Remove(EncodeKey(networkAddressKey));
        }

        public NetworkAddressWithTime this[NetworkAddressKey networkAddressKey]
        {
            get
            {
                NetworkAddressWithTime networkAddressWithTime;
                if (this.TryGetValue(networkAddressKey, out networkAddressWithTime))
                    return networkAddressWithTime;

                throw new KeyNotFoundException();
            }
            set
            {
                this.dict[EncodeKey(networkAddressKey).ToArray()] = EncodeValue(value);
            }
        }

        public void Flush()
        {
            this.dict.Flush();
        }

        public IEnumerator<KeyValuePair<NetworkAddressKey, NetworkAddressWithTime>> GetEnumerator()
        {
            foreach (var keyPair in this.dict)
                yield return new KeyValuePair<NetworkAddressKey, NetworkAddressWithTime>(DecodeKey(keyPair.Key), DecodeValue(keyPair.Value));
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        private static byte[] EncodeKey(NetworkAddressKey networkAddressKey)
        {
            return NodeEncoder.EncodeNetworkAddressKey(networkAddressKey);
        }

        private static byte[] EncodeValue(NetworkAddressWithTime networkAddressWithTime)
        {
            return NodeEncoder.EncodeNetworkAddressWithTime(networkAddressWithTime);
        }

        private static NetworkAddressKey DecodeKey(byte[] networkAddressKeyBytes)
        {
            return NodeEncoder.DecodeNetworkAddressKey(networkAddressKeyBytes);
        }

        private static NetworkAddressWithTime DecodeValue(byte[] networkAddressWithTimeBytes)
        {
            return NodeEncoder.DecodeNetworkAddressWithTime(networkAddressWithTimeBytes);
        }
    }
}
