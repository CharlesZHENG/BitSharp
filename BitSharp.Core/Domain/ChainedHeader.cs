using BitSharp.Common;
using System;
using System.Numerics;

namespace BitSharp.Core.Domain
{
    public class ChainedHeader
    {
        private readonly BlockHeader blockHeader;
        private readonly int height;
        private readonly BigInteger totalWork;

        public ChainedHeader(BlockHeader blockHeader, int height, BigInteger totalWork)
        {
            this.blockHeader = blockHeader;
            this.height = height;
            this.totalWork = totalWork;
        }

        public BlockHeader BlockHeader { get { return this.blockHeader; } }

        public UInt32 Version { get { return this.blockHeader.Version; } }

        public UInt256 PreviousBlockHash { get { return this.blockHeader.PreviousBlock; } }

        public UInt256 MerkleRoot { get { return this.blockHeader.MerkleRoot; } }

        public UInt32 Time { get { return this.blockHeader.Time; } }

        public UInt32 Bits { get { return this.blockHeader.Bits; } }

        public UInt32 Nonce { get { return this.blockHeader.Nonce; } }

        public UInt256 Hash { get { return this.blockHeader.Hash; } }

        public int Height { get { return this.height; } }

        public BigInteger TotalWork { get { return this.totalWork; } }

        public override bool Equals(object obj)
        {
            if (!(obj is ChainedHeader))
                return false;

            return (ChainedHeader)obj == this;
        }

        public override int GetHashCode()
        {
            return this.blockHeader.GetHashCode() ^ this.height.GetHashCode() ^ this.totalWork.GetHashCode();
        }

        public static bool operator ==(ChainedHeader left, ChainedHeader right)
        {
            return object.ReferenceEquals(left, right) || (!object.ReferenceEquals(left, null) && !object.ReferenceEquals(right, null) && left.blockHeader == right.blockHeader && left.height == right.height && left.totalWork == right.totalWork);
        }

        public static bool operator !=(ChainedHeader left, ChainedHeader right)
        {
            return !(left == right);
        }

        public static implicit operator BlockHeader(ChainedHeader chainedHeader)
        {
            return chainedHeader.BlockHeader;
        }

        public static ChainedHeader CreateForGenesisBlock(BlockHeader genesisBlockHeader)
        {
            return new ChainedHeader
            (
                blockHeader: genesisBlockHeader,
                height: 0,
                totalWork: genesisBlockHeader.CalculateWork()
            );
        }

        public static ChainedHeader CreateFromPrev(ChainedHeader prevChainedHeader, BlockHeader blockHeader)
        {
            var headerWork = blockHeader.CalculateWork();
            if (headerWork < 0)
                return null;

            return new ChainedHeader(blockHeader,
                prevChainedHeader.Height + 1,
                prevChainedHeader.TotalWork + headerWork);
        }
    }
}
