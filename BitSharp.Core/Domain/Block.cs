using BitSharp.Common;
using System.Collections.Immutable;

namespace BitSharp.Core.Domain
{
    public class Block
    {
        public Block(BlockHeader header, ImmutableArray<Transaction> transactions)
        {
            Header = header;
            Transactions = transactions;
        }

        public UInt256 Hash => this.Header.Hash;

        public BlockHeader Header { get; }

        public ImmutableArray<Transaction> Transactions { get; }

        public Block With(BlockHeader Header = null, ImmutableArray<Transaction>? Transactions = null)
        {
            return new Block
            (
                Header ?? this.Header,
                Transactions ?? this.Transactions
            );
        }
    }
}
