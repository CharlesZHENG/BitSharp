using BitSharp.Common;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;

namespace BitSharp.Core.Test.Rules
{
    public class UnitTestRules : MainnetRules
    {
        public static readonly UInt256 Target0 = UInt256.ParseHex("FFFFFF0000000000000000000000000000000000000000000000000000000000");
        public static readonly UInt256 Target1 = UInt256.ParseHex("0FFFFFF000000000000000000000000000000000000000000000000000000000");
        public static readonly UInt256 Target2 = UInt256.ParseHex("00FFFFFF00000000000000000000000000000000000000000000000000000000");
        public static readonly UInt256 Target3 = UInt256.ParseHex("000FFFFFF0000000000000000000000000000000000000000000000000000000");
        public static readonly UInt256 Target4 = UInt256.ParseHex("0000FFFFFF000000000000000000000000000000000000000000000000000000");

        private UInt256 _highestTarget;
        private Block _genesisBlock;
        private ChainedHeader _genesisChainedHeader;

        public UnitTestRules()
        {
            this._highestTarget = Target0;
        }

        public override UInt256 HighestTarget { get { return this._highestTarget; } }

        public override Block GenesisBlock { get { return this._genesisBlock; } }

        public override ChainedHeader GenesisChainedHeader { get { return this._genesisChainedHeader; } }

        public void SetGenesisBlock(Block genesisBlock)
        {
            this._genesisBlock = genesisBlock;
            this._genesisChainedHeader = ChainedHeader.CreateForGenesisBlock(this._genesisBlock.Header);
        }

        public void SetHighestTarget(UInt256 highestTarget)
        {
            this._highestTarget = highestTarget;
        }
    }
}
