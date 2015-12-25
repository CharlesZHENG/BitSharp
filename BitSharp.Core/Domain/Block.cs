using BitSharp.Common;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Core.Domain
{
    public class Block
    {
        private readonly Lazy<ImmutableArray<Transaction>> lazyTransactions;

        public Block(BlockHeader header, ImmutableArray<BlockTx> blockTxes)
        {
            Header = header;

            BlockTxes = blockTxes;

            lazyTransactions = new Lazy<ImmutableArray<Transaction>>(() =>
                ImmutableArray.CreateRange(blockTxes.Select(x => x.Decode().Transaction)));
        }

        public UInt256 Hash => this.Header.Hash;

        public BlockHeader Header { get; }

        public ImmutableArray<BlockTx> BlockTxes { get; }

        public ImmutableArray<Transaction> Transactions => lazyTransactions.Value;

        public Block With(BlockHeader Header = null, ImmutableArray<BlockTx>? BlockTxes = null)
        {
            return new Block
            (
                Header ?? this.Header,
                BlockTxes ?? this.BlockTxes
            );
        }

        public static Block Create(BlockHeader header, ImmutableArray<Transaction> transactions)
        {
            var blockTxes = ImmutableArray.CreateRange(transactions.Select((tx, txIndex) =>
                (BlockTx)BlockTx.Create(txIndex, tx)));

            return new Block(header, blockTxes);
        }

        public Block CreateWith(BlockHeader Header = null, ImmutableArray<Transaction>? Transactions = null)
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
                return Create(Header ?? this.Header, Transactions.Value);
            }
        }
    }
}
