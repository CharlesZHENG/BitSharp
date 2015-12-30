using BitSharp.Common;
using BitSharp.Core.Domain;
using System.Collections.Immutable;

namespace BitSharp.Core.Rules
{
    public enum ChainType
    {
        MainNet,
        Regtest,
        TestNet3
    }

    public interface IChainParams
    {
        ChainType ChainType { get; }

        UInt256 GenesisHash { get; }

        Block GenesisBlock { get; }

        ChainedHeader GenesisChainedHeader { get; }

        UInt256 HighestTarget { get; }

        int DifficultyInterval { get; }

        int DifficultyTargetTimespan { get; }

        bool AllowMininimumDifficultyBlocks { get; }

        bool PowNoRetargeting { get; }

        int PowTargetSpacing { get; }

        int MajorityWindow { get; }

        int MajorityEnforceBlockUpgrade { get; }

        int MajorityRejectBlockOutdated { get; }
    }
}
