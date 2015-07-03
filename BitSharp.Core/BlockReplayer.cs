using BitSharp.Common;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage;
using NLog;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
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

            // prepare a split/combine to load txes in parallel and return them in order
            TransformBlock<LoadingTx, LoadingTx> loadingTxSplitter; TransformManyBlock<LoadedTx, LoadedTx> loadedTxCombiner;
            SplitCombineBlock.Create<LoadingTx, LoadedTx, UInt256>(
                loadingTx => loadingTx.Transaction.Hash,
                loadedTx => loadedTx.Transaction.Hash,
                out loadingTxSplitter, out loadedTxCombiner);

            // capture the original order of the loading txes
            loadingTxes.LinkTo(loadingTxSplitter, new DataflowLinkOptions { PropagateCompletion = true });

            // begin loading txes
            var loadedTxes = TxLoader.LoadTxes(coreStorage, loadingTxSplitter, cancelToken);

            // sort loaded txes
            loadedTxes.LinkTo(loadedTxCombiner, new DataflowLinkOptions { PropagateCompletion = true });

            // return replay txes
            return loadedTxCombiner;
        }
    }
}
