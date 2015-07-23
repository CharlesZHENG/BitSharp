using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
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
            this.bits = DataCalculator.TargetToBits(UnitTestRules.Target0);
            this.nonce = (UInt32)Interlocked.Increment(ref staticNonce);
            this.totalWork = 0;
        }

        public FakeHeaders(IEnumerable<ChainedHeader> blockHeaders)
        {
            this.blockHeaders = ImmutableList.CreateRange(blockHeaders).ToBuilder();
            this.bits = DataCalculator.TargetToBits(UnitTestRules.Target0);
            this.nonce = (UInt32)Interlocked.Increment(ref staticNonce);
            this.totalWork = this.blockHeaders.Sum(x => x.BlockHeader.CalculateWork());
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

            var blockHeader = new BlockHeader(0, UInt256.Zero, UInt256.Zero, 0, this.bits, this.nonce);
            this.totalWork = blockHeader.CalculateWork();

            var chainedHeader = new ChainedHeader(blockHeader, 0, this.totalWork);
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

            var blockHeader = new BlockHeader(0, prevBlockHeader.Hash, UInt256.Zero, 0, bits ?? this.bits, this.nonce);
            this.totalWork += blockHeader.CalculateWork();

            var chainedHeader = new ChainedHeader(blockHeader, this.blockHeaders.Count, this.totalWork);
            this.blockHeaders.Add(chainedHeader);

            return chainedHeader;
        }

        public ChainedHeader this[int i] => this.blockHeaders[i];
    }
}
