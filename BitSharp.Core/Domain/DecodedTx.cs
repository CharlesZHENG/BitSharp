using System.Collections.Immutable;

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
