using BitSharp.Common;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Core.Domain
{
    public class BlockTx : MerkleTreeNode
    {
        private readonly Lazy<Transaction> lazyTx;

        public BlockTx(int index, int depth, UInt256 hash, bool pruned, ImmutableArray<byte>? txBytes)
            : base(index, depth, hash, pruned)
        {
            TxBytes = txBytes;
            lazyTx = new Lazy<Transaction>(() => txBytes != null ? DataEncoder.DecodeTransaction(txBytes.Value.ToArray()) : null);
        }

        [Obsolete]
        public BlockTx(int index, int depth, UInt256 hash, bool pruned, Transaction transaction)
            : base(index, depth, hash, pruned)
        {
            TxBytes = transaction != null ? (ImmutableArray<byte>?)DataEncoder.EncodeTransaction(transaction).ToImmutableArray() : null;
            lazyTx = new Lazy<Transaction>(() => transaction).Force();
        }

        //TODO only used by tests
        [Obsolete]
        public BlockTx(int txIndex, Transaction tx)
            : this(txIndex, 0, tx.Hash, false, tx)
        { }

        public bool IsCoinbase => this.Index == 0;

        public ImmutableArray<byte>? TxBytes { get; }

        [Obsolete]
        public Transaction Transaction => Decode();

        public Transaction Decode()
        {
            return lazyTx.Value;
        }
    }
}
