using BitSharp.Common;
using System;
using System.Collections.Immutable;
using System.Linq;
using System.IO;

namespace BitSharp.Core.Domain
{
    public class DecodedTx : EncodedTx
    {
        internal DecodedTx(ImmutableArray<byte> txBytes, Transaction tx)
            : base(txBytes, tx)
        {
            Transaction = tx;
        }

        public Transaction Transaction { get; }
    }
}
