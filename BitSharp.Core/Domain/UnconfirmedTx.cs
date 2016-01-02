using BitSharp.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BitSharp.Core.Domain
{
    public class UnconfirmedTx
    {
        public UnconfirmedTx(Transaction tx)
        {
            Transaction = tx;
        }

        public UInt256 Hash => Transaction.Hash;

        public Transaction Transaction { get; }
    }
}
