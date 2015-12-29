using BitSharp.Common;
using BitSharp.Core.Domain;
using System.Collections.Immutable;

namespace BitSharp.Core.Rules
{
    public interface IChainParams
    {
        UInt256 GenesisHash { get; }

        Block GenesisBlock { get; }

        ChainedHeader GenesisChainedHeader { get; }

        UInt256 HighestTarget { get; }

        int DifficultyInterval { get; }

        long DifficultyTargetTimespan { get; }
    }
}
