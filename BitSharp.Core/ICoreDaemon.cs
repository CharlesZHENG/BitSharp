using BitSharp.Core.Domain;

namespace BitSharp.Core
{
    public interface ICoreDaemon
    {
        /// <summary>
        /// Retrieve the chain for the current processed chain state.
        /// </summary>
        Chain CurrentChain { get; }

        IChainState GetChainState();
    }
}
