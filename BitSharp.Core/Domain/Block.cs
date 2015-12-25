using BitSharp.Common;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Core.Domain
{
    public class Block
    {
        private readonly Lazy<ImmutableArray<BlockTx>> lazyBlockTxes;
        private readonly Lazy<ImmutableArray<Transaction>> lazyTransactions;

        public Block(BlockHeader header, ImmutableArray<BlockTx> blockTxes)
        {
            Header = header;

            lazyTransactions = new Lazy<ImmutableArray<Transaction>>(() =>
                ImmutableArray.CreateRange(blockTxes.Select(x => x.Decode())));

            lazyBlockTxes = new Lazy<ImmutableArray<BlockTx>>(() => blockTxes).Force();
        }

        [Obsolete]
        public Block(BlockHeader header, ImmutableArray<Transaction> transactions)
        {
            Header = header;

            lazyTransactions = new Lazy<ImmutableArray<Transaction>>(() => transactions).Force();

            lazyBlockTxes = new Lazy<ImmutableArray<BlockTx>>(() =>
                ImmutableArray.CreateRange(transactions.Select((tx, txIndex) =>
                    new BlockTx(txIndex, 0, tx.Hash, false,
                        new EncodedTx(DataEncoder.EncodeTransaction(tx).ToImmutableArray(), tx)))));
        }

        public UInt256 Hash => this.Header.Hash;

        public BlockHeader Header { get; }

        public ImmutableArray<BlockTx> BlockTxes => lazyBlockTxes.Value;

        public ImmutableArray<Transaction> Transactions => lazyTransactions.Value;

        public Block With(BlockHeader Header = null, ImmutableArray<Transaction>? Transactions = null)
        {
            if (Transactions == null)
            {
                return new Block
                (
                    Header ?? this.Header,
                    this.BlockTxes
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
