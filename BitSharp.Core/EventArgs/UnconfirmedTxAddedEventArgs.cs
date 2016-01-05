using BitSharp.Core.Domain;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BitSharp.Core
{
    public class UnconfirmedTxAddedEventArgs : EventArgs
    {
        public UnconfirmedTxAddedEventArgs(UnconfirmedTx unconfirmedTx)
        {
            UnconfirmedTx = unconfirmedTx;
        }

        public UnconfirmedTx UnconfirmedTx { get; }
    }
}
