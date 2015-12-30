using BitSharp.Common;
using BitSharp.Core.Domain;

namespace BitSharp.Core.Rules
{
    public partial class Testnet3Params : IChainParams
    {
        private readonly MainnetParams mainnetParams = new MainnetParams();

        public Testnet3Params()
        {
            GenesisChainedHeader = ChainedHeader.CreateForGenesisBlock(genesisBlock.Header);
        }

        public UInt256 GenesisHash { get; } = UInt256.ParseHex("000000000933ea01ad0ee984209779baaec3ced90fa3f408719526f8d77f4943");

        public Block GenesisBlock => genesisBlock;

        public ChainedHeader GenesisChainedHeader { get; }

        public UInt256 HighestTarget { get; } = UInt256.ParseHex("00000000ffffffffffffffffffffffffffffffffffffffffffffffffffffffff");

        public int DifficultyInterval => mainnetParams.DifficultyInterval;

        public int DifficultyTargetTimespan => mainnetParams.DifficultyTargetTimespan;

        public bool AllowMininimumDifficultyBlocks { get; } = true;

        public bool PowNoRetargeting => mainnetParams.PowNoRetargeting;

        public int PowTargetSpacing => mainnetParams.PowTargetSpacing;

        public int MajorityWindow { get; } = 100;

        public int MajorityEnforceBlockUpgrade { get; } = 51;

        public int MajorityRejectBlockOutdated { get; } = 75;
    }
}
