using BitSharp.Common;
using System;
using System.Numerics;

namespace BitSharp.Core.Domain
{
    public class ChainedHeader
    {
        public ChainedHeader(BlockHeader blockHeader, int height, BigInteger totalWork)
        {
            BlockHeader = blockHeader;
            Height = height;
            TotalWork = totalWork;
        }

        public BlockHeader BlockHeader { get; }

        public UInt32 Version { get { return this.BlockHeader.Version; } }

        public UInt256 PreviousBlockHash { get { return this.BlockHeader.PreviousBlock; } }

        public UInt256 MerkleRoot { get { return this.BlockHeader.MerkleRoot; } }

        public UInt32 Time { get { return this.BlockHeader.Time; } }

        public UInt32 Bits { get { return this.BlockHeader.Bits; } }

        public UInt32 Nonce { get { return this.BlockHeader.Nonce; } }

        public UInt256 Hash { get { return this.BlockHeader.Hash; } }

        public int Height { get; }

        public BigInteger TotalWork { get; }

        public override bool Equals(object obj)
        {
            if (!(obj is ChainedHeader))
                return false;

            return (ChainedHeader)obj == this;
        }

        public override int GetHashCode()
        {
            return this.BlockHeader.GetHashCode() ^ this.Height.GetHashCode() ^ this.TotalWork.GetHashCode();
        }

        public static bool operator ==(ChainedHeader left, ChainedHeader right)
        {
            return object.ReferenceEquals(left, right) || (!object.ReferenceEquals(left, null) && !object.ReferenceEquals(right, null) && left.BlockHeader == right.BlockHeader && left.Height == right.Height && left.TotalWork == right.TotalWork);
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
