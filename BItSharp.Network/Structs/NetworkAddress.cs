﻿using BitSharp.Common;
using BitSharp.Network.ExtensionMethods;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;

namespace BitSharp.Network
{
    public class NetworkAddress
    {
        public readonly UInt64 Services;
        public readonly ImmutableList<byte> IPv6Address;
        public readonly UInt16 Port;

        public NetworkAddress(UInt64 Services, ImmutableList<byte> IPv6Address, UInt16 Port)
        {
            this.Services = Services;
            this.IPv6Address = IPv6Address;
            this.Port = Port;
        }

        public NetworkAddress With(UInt64? Services = null, ImmutableList<byte> IPv6Address = null, UInt16? Port = null)
        {
            return new NetworkAddress
            (
                Services ?? this.Services,
                IPv6Address ?? this.IPv6Address,
                Port ?? this.Port
            );
        }
    }
}
