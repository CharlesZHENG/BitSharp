using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Test.Rules;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Threading;

namespace BitSharp.Core.Test
{
    public class FakeHeaders
    {
        private static int staticNonce = 0;

        private readonly List<BlockHeader> blockHeaders;
        private readonly UInt32 bits;
        private readonly UInt32 nonce;

        private BigInteger totalWork;

        public FakeHeaders()
        {
            this.blockHeaders = new List<BlockHeader>();
            this.bits = DataCalculator.TargetToBits(UnitTestRules.Target0);
            this.nonce = (UInt32)Interlocked.Increment(ref staticNonce);
            this.totalWork = 0;
        }

        public FakeHeaders(IEnumerable<BlockHeader> blockHeaders)
        {
            this.blockHeaders = new List<BlockHeader>(blockHeaders);
            this.bits = DataCalculator.TargetToBits(UnitTestRules.Target0);
            this.nonce = (UInt32)Interlocked.Increment(ref staticNonce);
            this.totalWork = this.blockHeaders.Sum(x => x.CalculateWork());
        }

        public FakeHeaders(FakeHeaders parent)
            : this(parent.blockHeaders)
        {
        }

        public BlockHeader Genesis()
        {
            if (this.blockHeaders.Count > 0)
                throw new InvalidOperationException();

            var blockHeader = new BlockHeader(0, 0, 0, 0, this.bits, this.nonce);

            this.blockHeaders.Add(blockHeader);
            this.totalWork += blockHeader.CalculateWork();

            return blockHeader;
        }

        public ChainedHeader GenesisChained()
        {
            return new ChainedHeader(this.Genesis(), this.blockHeaders.Count - 1, this.totalWork);
        }

        public BlockHeader Next(UInt32? bits = null)
        {
            if (this.blockHeaders.Count == 0)
                throw new InvalidOperationException();

            var prevBlockHeader = this.blockHeaders.Last();
            var blockHeader = new BlockHeader(0, prevBlockHeader.Hash, 0, 0, bits ?? this.bits, this.nonce);

            this.blockHeaders.Add(blockHeader);
            this.totalWork += blockHeader.CalculateWork();

            return blockHeader;
        }

        public ChainedHeader NextChained(UInt32? bits = null)
        {
            return new ChainedHeader(this.Next(bits), this.blockHeaders.Count - 1, this.totalWork);
        }

        public BlockHeader this[int i]
        {
            get { return this.blockHeaders[i]; }
        }
    }
}
