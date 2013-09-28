﻿using BitSharp.Common;
using BitSharp.Network.ExtensionMethods;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace BitSharp.Network
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
