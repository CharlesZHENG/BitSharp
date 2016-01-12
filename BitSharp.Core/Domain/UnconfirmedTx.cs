using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BitSharp.Core.Domain
{
    public class UnconfirmedTx
    {
        public UnconfirmedTx(ValidatableTx tx, DateTimeOffset dateSeen)
        {
            ValidatableTx = tx;
            DateSeen = dateSeen;
            Fee = tx.PrevTxOutputs.Sum(x => x.Value) - tx.Transaction.Outputs.Sum(x => x.Value);
        }

        public UInt256 Hash => Transaction.Hash;

        public ValidatableTx ValidatableTx { get; }

        public Transaction Transaction => ValidatableTx.Transaction;

        public DateTimeOffset DateSeen { get; }

        public ulong Fee { get; }

        public int TxByteSize => ValidatableTx.TxBytes.Length;
    }
}
