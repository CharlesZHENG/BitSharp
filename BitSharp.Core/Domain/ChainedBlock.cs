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

        public int Height { get { return this.ChainedHeader.Height; } }

        public BigInteger TotalWork { get { return this.ChainedHeader.TotalWork; } }

        public UInt256 Hash { get { return this.Block.Hash; } }

        public BlockHeader Header { get { return this.Block.Header; } }

        public ImmutableArray<Transaction> Transactions { get { return this.Block.Transactions; } }

        public static implicit operator Block(ChainedBlock chainedBlock)
        {
            return chainedBlock.Block;
        }
    }
}
