using BitSharp.Common;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage;
using NLog;
using System.Threading;
using System.Threading.Tasks.Dataflow;

namespace BitSharp.Core.Builders
{
    public static class BlockReplayer
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        public static ISourceBlock<LoadedTx> ReplayBlock(ICoreStorage coreStorage, IChainState chainState, UInt256 blockHash, bool replayForward, CancellationToken cancelToken = default(CancellationToken))
        {
            ChainedHeader replayBlock;
            if (!coreStorage.TryGetChainedHeader(blockHash, out replayBlock))
                throw new MissingDataException(blockHash);

            // replay the loading txes for this block, in reverse order for a rollback
            ISourceBlock<LoadingTx> loadingTxes;
            if (replayForward)
                loadingTxes = UtxoReplayer.ReplayCalculateUtxo(coreStorage, chainState, replayBlock, cancelToken);
            else
                loadingTxes = UtxoReplayer.ReplayRollbackUtxo(coreStorage, chainState, replayBlock, cancelToken);

            // capture the original loading txes order
            var orderedLoadingTxes = OrderingBlock.CaptureOrder<LoadingTx, LoadedTx, UInt256>(
                loadingTxes, loadingTx => loadingTx.Transaction.Hash, cancelToken);

            // begin loading txes
            var loadedTxes = TxLoader.LoadTxes(coreStorage, orderedLoadingTxes, cancelToken);

            // return the loaded txes in original order
            return orderedLoadingTxes.ApplyOrder(loadedTxes, loadedTx => loadedTx.Transaction.Hash, cancelToken);
        }
    }
}
