using BitSharp.Common;
using BitSharp.Core.Domain;
using BitSharp.Core.Test.Rules;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Numerics;
using System.Threading;

namespace BitSharp.Core.Test
{
    public class FakeHeaders
    {
        private static int staticNonce = 0;

        private readonly ImmutableList<ChainedHeader>.Builder blockHeaders;
        private readonly UInt32 bits;
        private readonly UInt32 nonce;

        private BigInteger totalWork;

        public FakeHeaders()
        {
            this.blockHeaders = ImmutableList.CreateBuilder<ChainedHeader>();
            this.bits = DataCalculator.ToCompact(UnitTestParams.Target0);
            this.nonce = (UInt32)Interlocked.Increment(ref staticNonce);
            this.totalWork = 0;
        }

        public FakeHeaders(IEnumerable<ChainedHeader> blockHeaders)
        {
            this.blockHeaders = ImmutableList.CreateRange(blockHeaders).ToBuilder();
            this.bits = DataCalculator.ToCompact(UnitTestParams.Target0);
            this.nonce = (UInt32)Interlocked.Increment(ref staticNonce);
            this.totalWork = blockHeaders.LastOrDefault()?.TotalWork ?? 0;
        }

        public FakeHeaders(FakeHeaders parent)
            : this(parent.blockHeaders)
        {
        }

        public ImmutableList<ChainedHeader> ChainedHeaders => blockHeaders.ToImmutable();

        public BlockHeader Genesis()
        {
            return GenesisChained().BlockHeader;
        }

        public ChainedHeader GenesisChained()
        {
            if (this.blockHeaders.Count > 0)
                throw new InvalidOperationException();

            var blockHeader = BlockHeader.Create(0, UInt256.Zero, UInt256.Zero, DateTimeOffset.FromUnixTimeSeconds(0), this.bits, this.nonce);
            this.totalWork = blockHeader.CalculateWork().ToBigInteger();

            var chainedHeader = new ChainedHeader(blockHeader, 0, this.totalWork, DateTimeOffset.MinValue);
            this.blockHeaders.Add(chainedHeader);

            return chainedHeader;
        }

        public BlockHeader Next(UInt32? bits = null)
        {
            return NextChained(bits).BlockHeader;
        }

        public ChainedHeader NextChained(UInt32? bits = null)
        {
            if (this.blockHeaders.Count == 0)
                throw new InvalidOperationException();

            var prevBlockHeader = this.blockHeaders.Last();

            var blockHeader = BlockHeader.Create(0, prevBlockHeader.Hash, UInt256.Zero, DateTimeOffset.FromUnixTimeSeconds(0), bits ?? this.bits, this.nonce);
            this.totalWork += blockHeader.CalculateWork().ToBigInteger();

            var chainedHeader = new ChainedHeader(blockHeader, this.blockHeaders.Count, this.totalWork, DateTimeOffset.Now);
            this.blockHeaders.Add(chainedHeader);

            return chainedHeader;
        }

        public ChainedHeader this[int i] => this.blockHeaders[i];
    }
}
