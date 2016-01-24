using BitSharp.Core.Domain;
using System;

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
