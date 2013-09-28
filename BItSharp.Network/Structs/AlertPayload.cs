﻿using BitSharp.Common;
using BitSharp.Network.ExtensionMethods;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace BitSharp.Network
{
    public class AlertPayload
    {
        public readonly string Payload;
        public readonly string Signature;

        public AlertPayload(string Payload, string Signature)
        {
            this.Payload = Payload;
            this.Signature = Signature;
        }

        public AlertPayload With(string Payload = null, string Signature = null)
        {
            return new AlertPayload
            (
                Payload ?? this.Payload,
                Signature ?? this.Signature
            );
        }
    }
}
