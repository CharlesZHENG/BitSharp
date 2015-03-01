using System.Collections.Immutable;

namespace BitSharp.Node.Domain
{
    public class AddressPayload
    {
        public readonly ImmutableArray<NetworkAddressWithTime> NetworkAddresses;

        public AddressPayload(ImmutableArray<NetworkAddressWithTime> NetworkAddresses)
        {
            this.NetworkAddresses = NetworkAddresses;
        }

        public AddressPayload With(ImmutableArray<NetworkAddressWithTime>? NetworkAddresses = null)
        {
            return new AddressPayload
            (
                NetworkAddresses ?? this.NetworkAddresses
            );
        }
    }
}
