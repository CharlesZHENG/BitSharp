using BitSharp.Core.Domain;
using System;

namespace BitSharp.Core
{
    public interface ICoreDaemon
    {
        event EventHandler OnChainStateChanged;

        event EventHandler<UnconfirmedTxAddedEventArgs> UnconfirmedTxAdded;

        event EventHandler<TxesConfirmedEventArgs> TxesConfirmed;

        event EventHandler<TxesUnconfirmedEventArgs> TxesUnconfirmed;

        /// <summary>
        /// Retrieve the chain for the current processed chain state.
        /// </summary>
        Chain CurrentChain { get; }

        IChainState GetChainState();
    }
}
