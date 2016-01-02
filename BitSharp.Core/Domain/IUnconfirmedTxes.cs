using BitSharp.Common;
using System;
using System.Collections.Immutable;

namespace BitSharp.Core.Domain
{
    public interface IUnconfirmedTxes : IDisposable
    {
        int CursorCount { get; }

        Chain Chain { get; }

        bool ContainsTransaction(UInt256 txHash);

        bool TryGetTransaction(UInt256 txHash, out UnconfirmedTx unconfirmedTx);

        ImmutableDictionary<UInt256, UnconfirmedTx> GetTransactionsSpending(TxOutputKey txOutputKey);
    }
}