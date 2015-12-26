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

        public static ISourceBlock<ValidatableTx> ReplayBlock(ICoreStorage coreStorage, IChainState chainState, UInt256 blockHash, bool replayForward, CancellationToken cancelToken = default(CancellationToken))
        {
            ChainedHeader replayBlock;
            if (!coreStorage.TryGetChainedHeader(blockHash, out replayBlock))
                throw new MissingDataException(blockHash);

            // replay the validatable txes for this block, in reverse order for a rollback
            ISourceBlock<ValidatableTx> validatableTxes;
            if (replayForward)
                validatableTxes = UtxoReplayer.ReplayCalculateUtxo(coreStorage, chainState, replayBlock, cancelToken);
            else
                validatableTxes = UtxoReplayer.ReplayRollbackUtxo(coreStorage, chainState, replayBlock, cancelToken);

            return validatableTxes;
        }
    }
}
