using System;
using System.Collections.Immutable;

namespace BitSharp.Node.Domain
{
    public class Message
    {
        public readonly UInt32 Magic;
        public readonly string Command;
        public readonly UInt32 PayloadSize;
        public readonly UInt32 PayloadChecksum;
        public readonly ImmutableArray<byte> Payload;

        public Message(UInt32 Magic, string Command, UInt32 PayloadSize, UInt32 PayloadChecksum, ImmutableArray<byte> Payload)
        {
            this.Magic = Magic;
            this.Command = Command;
            this.PayloadSize = PayloadSize;
            this.PayloadChecksum = PayloadChecksum;
            this.Payload = Payload;
        }
    }
}
