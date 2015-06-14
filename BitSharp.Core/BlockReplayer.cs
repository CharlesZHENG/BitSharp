using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Core.Builders
{
    public class BlockReplayer : IDisposable
    {
        private readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly CoreStorage coreStorage;
        private readonly IBlockchainRules rules;

        private readonly PendingTxLoader pendingTxLoader;
        private readonly PrevTxLoader prevTxLoader;

        private IChainState chainState;
        private ChainedHeader replayBlock;
        private bool replayForward;
        private ImmutableDictionary<UInt256, UnmintedTx> unmintedTxes;
        private IDisposable pendingTxLoaderStopper;
        private IDisposable prevTxLoaderStopper;

        public BlockReplayer(CoreStorage coreStorage, IBlockchainRules rules)
        {
            this.coreStorage = coreStorage;
            this.rules = rules;

            // thread count for i/o task (TxLoader)
            var ioThreadCount = 4;

            this.pendingTxLoader = new PendingTxLoader("BlockReplayer", ioThreadCount);
            this.prevTxLoader = new PrevTxLoader("BlockReplayer", null, coreStorage, ioThreadCount);
        }

        public void Dispose()
        {
            this.pendingTxLoader.Dispose();
            this.prevTxLoader.Dispose();
        }

        public IDisposable StartReplay(IChainState chainState, UInt256 blockHash, bool replayForward)
        {
            this.chainState = chainState;
            this.replayBlock = this.coreStorage.GetChainedHeader(blockHash);
            this.replayForward = replayForward;

            if (replayForward)
            {
                this.unmintedTxes = null;
            }
            else
            {
                IImmutableList<UnmintedTx> unmintedTxesList;
                if (!this.chainState.TryGetBlockUnmintedTxes(this.replayBlock.Hash, out unmintedTxesList))
                {
                    throw new MissingDataException(this.replayBlock.Hash);
                }

                this.unmintedTxes = ImmutableDictionary.CreateRange(
                    unmintedTxesList.Select(x => new KeyValuePair<UInt256, UnmintedTx>(x.TxHash, x)));
            }

            IEnumerable<BlockTx> blockTxes;
            if (!this.coreStorage.TryReadBlockTransactions(this.replayBlock.Hash, this.replayBlock.MerkleRoot, /*requireTransaction:*/true, out blockTxes))
            {
                throw new MissingDataException(this.replayBlock.Hash);
            }

            this.pendingTxLoaderStopper = this.pendingTxLoader.StartLoading(chainState, replayBlock, replayForward, blockTxes, unmintedTxes);
            this.prevTxLoaderStopper = this.prevTxLoader.StartLoading(this.pendingTxLoader.GetQueue());

            return new DisposeAction(StopReplay);
        }

        //TODO result should indicate whether block was played forwards or rolled back
        public IEnumerable<LoadedTx> ReplayBlock()
        {
            foreach (var tx in this.prevTxLoader.GetQueue().GetConsumingEnumerable())
                yield return tx;

            // wait for loaders to finish, any exceptions will be rethrown here
            this.pendingTxLoader.WaitToComplete();
            this.prevTxLoader.WaitToComplete();
        }

        private void StopReplay()
        {
            this.pendingTxLoaderStopper.Dispose();
            this.prevTxLoaderStopper.Dispose();

            this.chainState = null;
            this.replayBlock = null;
            this.unmintedTxes = null;
            this.pendingTxLoaderStopper = null;
            this.prevTxLoaderStopper = null;
        }
    }
}
