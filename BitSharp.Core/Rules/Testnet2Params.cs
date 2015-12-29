using BitSharp.Common;
using BitSharp.Core.Domain;

namespace BitSharp.Core.Rules
{
    public partial class Testnet2Params : IChainParams
    {
        public Testnet2Params()
        {
            GenesisChainedHeader = ChainedHeader.CreateForGenesisBlock(genesisBlock.Header);
        }

        public UInt256 GenesisHash => UInt256.ParseHex("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206");

        public Block GenesisBlock => genesisBlock;

        public ChainedHeader GenesisChainedHeader { get; }

        public UInt256 HighestTarget { get; } = UInt256.ParseHex("7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");

        public int DifficultyInterval { get; } = 2016;

        public long DifficultyTargetTimespan { get; } = 14 * 24 * 60 * 60;
    }
}
