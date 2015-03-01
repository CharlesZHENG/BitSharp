using BitSharp.Common;
using System.Collections.Immutable;
using System.Numerics;

namespace BitSharp.Core.Domain
{
    public class ChainedBlock
    {
        private readonly ChainedHeader chainedHeader;
        private readonly Block block;

        public ChainedBlock(ChainedHeader chainedHeader, Block block)
        {
            this.chainedHeader = chainedHeader;
            this.block = block;
        }

        public ChainedHeader ChainedHeader { get { return this.chainedHeader; } }

        public Block Block { get { return this.block; } }

        public int Height { get { return this.chainedHeader.Height; } }

        public BigInteger TotalWork { get { return this.chainedHeader.TotalWork; } }

        public UInt256 Hash { get { return this.block.Hash; } }

        public BlockHeader Header { get { return this.block.Header; } }

        public ImmutableArray<Transaction> Transactions { get { return this.block.Transactions; } }

        public static implicit operator Block(ChainedBlock chainedBlock)
        {
            return chainedBlock.Block;
        }
    }
}
