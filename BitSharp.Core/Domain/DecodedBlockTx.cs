using BitSharp.Common;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Core.Domain
{
    public class DecodedBlockTx : BlockTx
    {
        public DecodedBlockTx(int index, DecodedTx tx)
            : base(index, tx)
        {
            DecodedTx = tx;
            Transaction = tx.Transaction;
        }

        public DecodedTx DecodedTx { get; }

        public Transaction Transaction { get; }

        public bool IsCoinbase => Transaction.IsCoinbase;

        public static implicit operator BlockTxNode(DecodedBlockTx tx)
        {
            return new BlockTxNode(tx.Index, 0, tx.Hash, false, tx.EncodedTx);
        }
    }
}
