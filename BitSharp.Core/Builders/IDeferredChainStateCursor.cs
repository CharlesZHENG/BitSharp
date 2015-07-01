using BitSharp.Common;
using BitSharp.Core.Storage;

namespace BitSharp.Core.Builders
{
    internal interface IDeferredChainStateCursor : IChainStateCursor
    {
        void WarmUnspentTx(UInt256 txHash);

        void ApplyChangesToParent(IChainStateCursor parent);
    }
}
