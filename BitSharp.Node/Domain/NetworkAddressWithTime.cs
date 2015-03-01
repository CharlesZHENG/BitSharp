using System;

namespace BitSharp.Node.Domain
{
    public class NetworkAddressWithTime
    {
        public readonly UInt32 Time;
        public readonly NetworkAddress NetworkAddress;

        public NetworkAddressWithTime(UInt32 Time, NetworkAddress NetworkAddress)
        {
            this.Time = Time;
            this.NetworkAddress = NetworkAddress;
        }

        public NetworkAddressWithTime With(UInt32? Time = null, NetworkAddress NetworkAddress = null)
        {
            return new NetworkAddressWithTime
            (
                Time ?? this.Time,
                NetworkAddress ?? this.NetworkAddress
            );
        }
    }
}
