using BitSharp.Common;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Core.Domain
{
    public class Block
    {
        private readonly Lazy<ImmutableArray<EncodedTx>> lazyEncodedTxes;
        private readonly Lazy<ImmutableArray<Transaction>> lazyTransactions;

        public Block(BlockHeader header, ImmutableArray<EncodedTx> encodedTxes)
        {
            Header = header;

            lazyTransactions = new Lazy<ImmutableArray<Transaction>>(() =>
                ImmutableArray.CreateRange(encodedTxes.Select(x => x.Decode())));

            lazyEncodedTxes = new Lazy<ImmutableArray<EncodedTx>>(() => encodedTxes).Force();
        }

        [Obsolete]
        public Block(BlockHeader header, ImmutableArray<Transaction> transactions)
        {
            Header = header;

            lazyTransactions = new Lazy<ImmutableArray<Transaction>>(() => transactions).Force();

            lazyEncodedTxes = new Lazy<ImmutableArray<EncodedTx>>(() =>
                ImmutableArray.CreateRange(transactions.Select(x =>
                    new EncodedTx(DataEncoder.EncodeTransaction(x).ToImmutableArray(), x))));
        }

        public UInt256 Hash => this.Header.Hash;

        public BlockHeader Header { get; }

        public ImmutableArray<EncodedTx> EncodedTxes => lazyEncodedTxes.Value;

        public ImmutableArray<Transaction> Transactions => lazyTransactions.Value;

        public Block With(BlockHeader Header = null, ImmutableArray<Transaction>? Transactions = null)
        {
            if (Transactions == null)
            {
                return new Block
                (
                    Header ?? this.Header,
                    this.EncodedTxes
                );
            }
            else
            {
                return new Block
                (
                    Header ?? this.Header,
                    Transactions.Value
                );
            }
        }
    }
}
