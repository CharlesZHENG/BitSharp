using System.Collections.Immutable;

namespace BitSharp.Core.Domain
{
    public class BlockchainPath
    {
        public BlockchainPath(ChainedHeader fromBlock, ChainedHeader toBlock, ChainedHeader lastCommonBlock, ImmutableList<ChainedHeader> rewindBlocks, ImmutableList<ChainedHeader> advanceBlocks)
        {
            FromBlock = fromBlock;
            ToBlock = toBlock;
            LastCommonBlock = lastCommonBlock;
            RewindBlocks = rewindBlocks;
            AdvanceBlocks = advanceBlocks;
        }

        public ChainedHeader FromBlock { get; }

        public ChainedHeader ToBlock { get; }

        public ChainedHeader LastCommonBlock { get; }

        public ImmutableList<ChainedHeader> RewindBlocks { get; }

        public ImmutableList<ChainedHeader> AdvanceBlocks { get; }

        public static BlockchainPath CreateSingleBlockPath(ChainedHeader chainedHeader)
        {
            return new BlockchainPath(chainedHeader, chainedHeader, chainedHeader, ImmutableList.Create<ChainedHeader>(), ImmutableList.Create<ChainedHeader>());
        }
    }
}
