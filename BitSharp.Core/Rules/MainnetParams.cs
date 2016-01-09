using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Script;
using NLog;
using System;
using System.Collections.Immutable;
using System.Linq;
using System.Numerics;

namespace BitSharp.Core.Rules
{
    public partial class MainnetParams : IChainParams
    {
        public MainnetParams()
        {
            GenesisChainedHeader = ChainedHeader.CreateForGenesisBlock(genesisBlock.Header);
        }

        public ChainType ChainType { get; } = ChainType.MainNet;

        public UInt256 GenesisHash { get; } = UInt256.ParseHex("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f");

        public Block GenesisBlock => genesisBlock;

        public ChainedHeader GenesisChainedHeader { get; }

        public UInt256 HighestTarget { get; } = UInt256.ParseHex("00000000ffffffffffffffffffffffffffffffffffffffffffffffffffffffff");

        // 2 weeks in blocks
        public int DifficultyInterval { get; } = 2016;

        public TimeSpan DifficultyTargetTimespan { get; } = TimeSpan.FromDays(14);

        public bool AllowMininimumDifficultyBlocks { get; } = false;

        public bool PowNoRetargeting { get; } = false;

        public TimeSpan PowTargetSpacing { get; } = TimeSpan.FromMinutes(10);

        public int MajorityWindow { get; } = 1000;

        public int MajorityEnforceBlockUpgrade { get; } = 750;

        public int MajorityRejectBlockOutdated { get; } = 950;
    }
}
