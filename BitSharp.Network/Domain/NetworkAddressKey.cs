using BitSharp.Common;
using System;
using System.Collections.Immutable;
using System.Data.HashFunction;
using System.Linq;

namespace BitSharp.Network.Domain
{
    public class NetworkAddressKey
    {
        private readonly int hashCode;

        public NetworkAddressKey(ImmutableArray<byte> IPv6Address, UInt16 Port)
        {
            this.IPv6Address = IPv6Address;
            this.Port = Port;

            var hashBytes = new byte[2 + IPv6Address.Length];
            Bits.EncodeUInt16(Port, hashBytes);
            IPv6Address.CopyTo(hashBytes, 2);
            hashCode = Bits.ToInt32(new xxHash(32).ComputeHash(hashBytes));
        }

        public ImmutableArray<byte> IPv6Address { get; }

        public UInt16 Port { get; }

        public override bool Equals(object obj)
        {
            if (!(obj is NetworkAddressKey))
                return false;

            var other = (NetworkAddressKey)obj;
            return other.IPv6Address.SequenceEqual(this.IPv6Address) && other.Port == this.Port;
        }

        public override int GetHashCode() => hashCode;
    }
}
