using BitSharp.Common;
using BitSharp.Core.Domain;

namespace BitSharp.Core.Rules
{
    public partial class Testnet3Params : IChainParams
    {
        public Testnet3Params()
        {
            GenesisChainedHeader = ChainedHeader.CreateForGenesisBlock(genesisBlock.Header);
        }

        public UInt256 GenesisHash { get; } = UInt256.ParseHex("000000000933ea01ad0ee984209779baaec3ced90fa3f408719526f8d77f4943");

        public Block GenesisBlock => genesisBlock;

        public ChainedHeader GenesisChainedHeader { get; }

        public UInt256 HighestTarget { get; } = UInt256.ParseHex("00000000ffffffffffffffffffffffffffffffffffffffffffffffffffffffff");

        public int DifficultyInterval { get; } = 2016;

        public long DifficultyTargetTimespan { get; } = 14 * 24 * 60 * 60;
    }
}
