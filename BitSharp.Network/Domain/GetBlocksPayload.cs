using BitSharp.Common;
using System;
using System.Collections.Immutable;

namespace BitSharp.Network.Domain
{
    public class GetBlocksPayload
    {
        public readonly UInt32 Version;
        public readonly ImmutableArray<UInt256> BlockLocatorHashes;
        public readonly UInt256 HashStop;

        public GetBlocksPayload(UInt32 Version, ImmutableArray<UInt256> BlockLocatorHashes, UInt256 HashStop)
        {
            this.Version = Version;
            this.BlockLocatorHashes = BlockLocatorHashes;
            this.HashStop = HashStop;
        }
    }
}
