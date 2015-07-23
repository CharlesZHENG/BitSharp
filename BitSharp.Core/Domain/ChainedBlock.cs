using BitSharp.Common;
using System.Collections.Immutable;
using System.Numerics;

namespace BitSharp.Core.Domain
{
    public class ChainedBlock
    {
        public ChainedBlock(ChainedHeader chainedHeader, Block block)
        {
            ChainedHeader = chainedHeader;
            Block = block;
        }

        public ChainedHeader ChainedHeader { get; }

        public Block Block { get; }

        public int Height => this.ChainedHeader.Height;

        public BigInteger TotalWork => this.ChainedHeader.TotalWork;

        public UInt256 Hash => this.Block.Hash;

        public BlockHeader Header => this.Block.Header;

        public ImmutableArray<Transaction> Transactions => this.Block.Transactions;

        public static implicit operator Block(ChainedBlock chainedBlock)
        {
            return chainedBlock.Block;
        }
    }
}
