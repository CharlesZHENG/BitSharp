using System;

namespace BitSharp.Network.Domain
{
    public class NetworkAddressWithTime
    {
        public readonly DateTimeOffset Time;
        public readonly NetworkAddress NetworkAddress;

        public NetworkAddressWithTime(DateTimeOffset Time, NetworkAddress NetworkAddress)
        {
            this.Time = Time;
            this.NetworkAddress = NetworkAddress;
        }

        public NetworkAddressWithTime With(DateTimeOffset? Time = null, NetworkAddress NetworkAddress = null)
        {
            return new NetworkAddressWithTime
            (
                Time ?? this.Time,
                NetworkAddress ?? this.NetworkAddress
            );
        }
    }
}
