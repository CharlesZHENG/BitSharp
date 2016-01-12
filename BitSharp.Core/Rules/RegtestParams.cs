using BitSharp.Common;
using BitSharp.Core.Domain;
using System;

namespace BitSharp.Core.Rules
{
    public partial class RegtestParams : IChainParams
    {
        private readonly MainnetParams mainnetParams = new MainnetParams();

        public RegtestParams()
        {
            GenesisChainedHeader = ChainedHeader.CreateForGenesisBlock(genesisBlock.Header);
        }

        public ChainType ChainType { get; } = ChainType.Regtest;

        public UInt256 GenesisHash => UInt256.ParseHex("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206");

        public Block GenesisBlock => genesisBlock;

        public ChainedHeader GenesisChainedHeader { get; }

        public UInt256 HighestTarget { get; } = UInt256.ParseHex("7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");

        public int DifficultyInterval => mainnetParams.DifficultyInterval;

        public TimeSpan DifficultyTargetTimespan => mainnetParams.DifficultyTargetTimespan;

        public bool AllowMininimumDifficultyBlocks { get; } = true;

        public bool PowNoRetargeting { get; } = true;

        public TimeSpan PowTargetSpacing => mainnetParams.PowTargetSpacing;

        public int MajorityWindow => mainnetParams.MajorityWindow;

        public int MajorityEnforceBlockUpgrade => mainnetParams.MajorityEnforceBlockUpgrade;

        public int MajorityRejectBlockOutdated => mainnetParams.MajorityRejectBlockOutdated;
    }
}
