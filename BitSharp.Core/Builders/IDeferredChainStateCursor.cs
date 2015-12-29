using BitSharp.Common;
using BitSharp.Core.Storage;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BitSharp.Core.Builders
{
    public interface IDeferredChainStateCursor : IChainStateCursor
    {
        int CursorCount { get; }

        void WarmUnspentTx(UInt256 txHash);

        IDataflowBlock UtxoWorkQueue { get; }

        IDataflowBlock UtxoApplierBlock { get; }

        Task ApplyChangesAsync();
    }
}
