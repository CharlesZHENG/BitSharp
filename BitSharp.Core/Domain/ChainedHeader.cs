using BitSharp.Common;
using System;
using System.Numerics;

namespace BitSharp.Core.Domain
{
    public class ChainedHeader
    {
        public ChainedHeader(BlockHeader blockHeader, int height, BigInteger totalWork, DateTime dateSeen)
        {
            BlockHeader = blockHeader;
            Height = height;
            TotalWork = totalWork;
            DateSeen = dateSeen;
        }

        public BlockHeader BlockHeader { get; }

        public UInt32 Version => this.BlockHeader.Version;

        public UInt256 PreviousBlockHash => this.BlockHeader.PreviousBlock;

        public UInt256 MerkleRoot => this.BlockHeader.MerkleRoot;

        public UInt32 Time => this.BlockHeader.Time;

        public UInt32 Bits => this.BlockHeader.Bits;

        public UInt32 Nonce => this.BlockHeader.Nonce;

        public UInt256 Hash => this.BlockHeader.Hash;

        public int Height { get; }

        public BigInteger TotalWork { get; }

        public DateTime DateSeen { get; }

        public override bool Equals(object obj)
        {
            if (!(obj is ChainedHeader))
                return false;

            return (ChainedHeader)obj == this;
        }

        public override int GetHashCode()
        {
            return this.BlockHeader.GetHashCode() ^ this.Height.GetHashCode() ^ this.TotalWork.GetHashCode() ^ this.DateSeen.GetHashCode();
        }

        public static bool operator ==(ChainedHeader left, ChainedHeader right)
        {
            return object.ReferenceEquals(left, right) || (!object.ReferenceEquals(left, null) && !object.ReferenceEquals(right, null) && left.BlockHeader == right.BlockHeader && left.Height == right.Height && left.TotalWork == right.TotalWork && left.DateSeen == right.DateSeen);
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
                totalWork: genesisBlockHeader.CalculateWork(),
                dateSeen: DateTime.MinValue
            );
        }

        public static ChainedHeader CreateFromPrev(ChainedHeader prevChainedHeader, BlockHeader blockHeader, DateTime dateSeen)
        {
            var headerWork = blockHeader.CalculateWork();
            if (headerWork < 0)
                return null;

            return new ChainedHeader(blockHeader,
                prevChainedHeader.Height + 1,
                prevChainedHeader.TotalWork + headerWork,
                dateSeen);
        }
    }
}
