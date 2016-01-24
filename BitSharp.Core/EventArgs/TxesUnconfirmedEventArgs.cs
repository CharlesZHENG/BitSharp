using BitSharp.Common;
using BitSharp.Core.Domain;
using System;
using System.Collections.Immutable;

namespace BitSharp.Core
{
    public class TxesUnconfirmedEventArgs : EventArgs
    {
        public TxesUnconfirmedEventArgs(ChainedHeader confirmBlock, ImmutableDictionary<UInt256, BlockTx> unconfirmedTxes)
        {
            UnconfirmBlock = confirmBlock;
            UnconfirmedTxes = unconfirmedTxes;
        }

        public ChainedHeader UnconfirmBlock { get; }

        public ImmutableDictionary<UInt256, BlockTx> UnconfirmedTxes { get; }
    }
}
