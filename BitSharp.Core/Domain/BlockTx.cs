using BitSharp.Common;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Core.Domain
{
    public class BlockTx : MerkleTreeNode
    {
        public BlockTx(int index, int depth, UInt256 hash, bool pruned, ImmutableArray<byte>? txBytes)
            : base(index, depth, hash, pruned)
        {
            EncodedTx = txBytes != null ? new EncodedTx(hash, txBytes.Value) : null;
        }

        public BlockTx(int index, int depth, UInt256 hash, bool pruned, EncodedTx rawTx)
            : base(index, depth, hash, pruned)
        {
            EncodedTx = rawTx;
        }

        [Obsolete]
        public BlockTx(int index, int depth, UInt256 hash, bool pruned, Transaction transaction)
            : base(index, depth, hash, pruned)
        {
            if (transaction != null)
            {
                var txBytes = DataEncoder.EncodeTransaction(transaction).ToImmutableArray();
                EncodedTx = new EncodedTx(txBytes, transaction);
            }
            else
            {
                EncodedTx = null;
            }
        }

        //TODO only used by tests
        [Obsolete]
        public BlockTx(int txIndex, Transaction tx)
            : this(txIndex, 0, tx.Hash, false, tx)
        { }

        public bool IsCoinbase => this.Index == 0;

        public EncodedTx EncodedTx { get; }

        [Obsolete]
        public Transaction Transaction => Decode();

        public Transaction Decode()
        {
            return EncodedTx?.Decode();
        }
    }
}
