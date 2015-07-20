using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage;
using NLog;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BitSharp.Core.Builders
{
    internal class UtxoBuilder
    {
        private static readonly int DUPE_COINBASE_1_HEIGHT = 91722;
        private static readonly UInt256 DUPE_COINBASE_1_HASH = UInt256.ParseHex("e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468");
        private static readonly int DUPE_COINBASE_2_HEIGHT = 91812;
        private static readonly UInt256 DUPE_COINBASE_2_HASH = UInt256.ParseHex("d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599");

        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        public ISourceBlock<LoadingTx> CalculateUtxo(IChainStateCursor chainStateCursor, Chain chain, ISourceBlock<BlockTx> blockTxes, CancellationToken cancelToken = default(CancellationToken))
        {
            var chainedHeader = chain.LastBlock;
            var blockSpentTxes = new BlockSpentTxesBuilder();

            var loadingTxes = new BufferBlock<LoadingTx>();

            var utxoCalculator = new ActionBlock<BlockTx>(
                blockTx =>
                {
                    var tx = blockTx.Transaction;
                    var txIndex = blockTx.Index;

                    var prevOutputTxKeys = ImmutableArray.CreateBuilder<TxLookupKey>(!blockTx.IsCoinbase ? tx.Inputs.Length : 0);

                    //TODO apply real coinbase rule
                    // https://github.com/bitcoin/bitcoin/blob/481d89979457d69da07edd99fba451fd42a47f5c/src/core.h#L219
                    if (!blockTx.IsCoinbase)
                    {
                        // spend each of the transaction's inputs in the utxo
                        for (var inputIndex = 0; inputIndex < tx.Inputs.Length; inputIndex++)
                        {
                            var input = tx.Inputs[inputIndex];
                            var unspentTx = this.Spend(chainStateCursor, txIndex, tx, inputIndex, input, chainedHeader, blockSpentTxes);

                            var unspentTxBlockHash = chain.Blocks[unspentTx.BlockIndex].Hash;
                            prevOutputTxKeys.Add(new TxLookupKey(unspentTxBlockHash, unspentTx.TxIndex));
                        }
                    }

                    // there exist two duplicate coinbases in the blockchain, which the design assumes to be impossible
                    // ignore the first occurrences of these duplicates so that they do not need to later be deleted from the utxo, an unsupported operation
                    // no other duplicates will occur again, it is now disallowed
                    var isDupeCoinbase = ((chainedHeader.Height == DUPE_COINBASE_1_HEIGHT && tx.Hash == DUPE_COINBASE_1_HASH)
                        || (chainedHeader.Height == DUPE_COINBASE_2_HEIGHT && tx.Hash == DUPE_COINBASE_2_HASH));

                    // add transaction's outputs to utxo, except for the genesis block and the duplicate coinbases
                    if (chainedHeader.Height > 0 && !isDupeCoinbase)
                    {
                        // mint the transaction's outputs in the utxo
                        this.Mint(chainStateCursor, tx, txIndex, chainedHeader);

                        // increase unspent output count
                        chainStateCursor.UnspentOutputCount += tx.Outputs.Length;

                        // increment unspent tx count
                        chainStateCursor.UnspentTxCount++;

                        chainStateCursor.TotalTxCount++;
                        chainStateCursor.TotalInputCount += tx.Inputs.Length;
                        chainStateCursor.TotalOutputCount += tx.Outputs.Length;
                    }

                    loadingTxes.Post(new LoadingTx(txIndex, tx, chainedHeader, prevOutputTxKeys.MoveToImmutable()));
                },
                new ExecutionDataflowBlockOptions { CancellationToken = cancelToken });

            blockTxes.LinkTo(utxoCalculator, new DataflowLinkOptions { PropagateCompletion = true });

            utxoCalculator.Completion.ContinueWith(
                task =>
                {
                    try
                    {
                        if (task.Status == TaskStatus.RanToCompletion)
                            if (!chainStateCursor.TryAddBlockSpentTxes(chainedHeader.Height, blockSpentTxes.ToImmutable()))
                                throw new ValidationException(chainedHeader.Hash);

                        if (task.IsCanceled)
                            ((IDataflowBlock)loadingTxes).Fault(new OperationCanceledException());
                        else if (task.IsFaulted)
                            ((IDataflowBlock)loadingTxes).Fault(task.Exception);
                        else
                            loadingTxes.Complete();
                    }
                    catch (Exception ex)
                    {
                        ((IDataflowBlock)loadingTxes).Fault(ex);
                        throw;
                    }
                }, cancelToken);

            return loadingTxes;
        }

        private void Mint(IChainStateCursor chainStateCursor, Transaction tx, int txIndex, ChainedHeader chainedHeader)
        {
            // add transaction to the utxo
            var unspentTx = new UnspentTx(tx.Hash, chainedHeader.Height, txIndex, tx.Outputs.Length, OutputState.Unspent);
            if (!chainStateCursor.TryAddUnspentTx(unspentTx))
            {
                // duplicate transaction
                logger.Warn("Duplicate transaction at block {0:#,##0}, {1}, coinbase".Format2(chainedHeader.Height, chainedHeader.Hash.ToHexNumberString()));
                throw new ValidationException(chainedHeader.Hash);
            }
        }

        private UnspentTx Spend(IChainStateCursor chainStateCursor, int txIndex, Transaction tx, int inputIndex, TxInput input, ChainedHeader chainedHeader, BlockSpentTxesBuilder blockSpentTxes)
        {
            UnspentTx unspentTx;
            if (!chainStateCursor.TryGetUnspentTx(input.PreviousTxOutputKey.TxHash, out unspentTx))
            {
                // output wasn't present in utxo, invalid block
                throw new ValidationException(chainedHeader.Hash);
            }

            var outputIndex = unchecked((int)input.PreviousTxOutputKey.TxOutputIndex);

            if (outputIndex < 0 || outputIndex >= unspentTx.OutputStates.Length)
            {
                // output was out of bounds
                throw new ValidationException(chainedHeader.Hash);
            }

            if (unspentTx.OutputStates[outputIndex] == OutputState.Spent)
            {
                // output was already spent
                throw new ValidationException(chainedHeader.Hash);
            }

            // update output states
            unspentTx = unspentTx.SetOutputState(outputIndex, OutputState.Spent);

            // decrement unspent output count
            chainStateCursor.UnspentOutputCount--;

            // update transaction output states in the utxo
            var wasUpdated = chainStateCursor.TryUpdateUnspentTx(unspentTx);
            if (!wasUpdated)
                throw new ValidationException(chainedHeader.Hash);

            // store pruning information for a fully spent transaction
            if (unspentTx.IsFullySpent)
            {
                blockSpentTxes.AddSpentTx(unspentTx.ToSpentTx());

                // decrement unspent tx count
                chainStateCursor.UnspentTxCount--;
            }

            return unspentTx;
        }

        //TODO with the rollback information that's now being stored, rollback could be down without needing the block
        public void RollbackUtxo(IChainStateCursor chainStateCursor, Chain chain, ChainedHeader chainedHeader, IEnumerable<BlockTx> blockTxes, ImmutableList<UnmintedTx>.Builder unmintedTxes)
        {
            //TODO don't reverse here, storage should be read in reverse
            foreach (var blockTx in blockTxes.Reverse())
            {
                var tx = blockTx.Transaction;
                var txIndex = blockTx.Index;

                // remove transaction outputs
                this.Unmint(chainStateCursor, tx, chainedHeader, isCoinbase: true);

                // decrease unspent output count
                chainStateCursor.UnspentOutputCount -= tx.Outputs.Length;

                // decrement unspent tx count
                chainStateCursor.UnspentTxCount--;

                chainStateCursor.TotalTxCount--;
                chainStateCursor.TotalInputCount -= tx.Inputs.Length;
                chainStateCursor.TotalOutputCount -= tx.Outputs.Length;

                var prevOutputTxKeys = ImmutableArray.CreateBuilder<TxLookupKey>(!blockTx.IsCoinbase ? tx.Inputs.Length : 0);

                if (!blockTx.IsCoinbase)
                {
                    // remove inputs in reverse order
                    for (var inputIndex = tx.Inputs.Length - 1; inputIndex >= 0; inputIndex--)
                    {
                        var input = tx.Inputs[inputIndex];
                        var unspentTx = this.Unspend(chainStateCursor, input, chainedHeader);

                        // store rollback replay information
                        var unspentTxBlockHash = chain.Blocks[unspentTx.BlockIndex].Hash;
                        prevOutputTxKeys.Add(new TxLookupKey(unspentTxBlockHash, unspentTx.TxIndex));
                    }
                }

                // reverse output keys to match original input order, as the inputs were read in reverse here
                prevOutputTxKeys.Reverse();

                // store rollback replay information
                unmintedTxes.Add(new UnmintedTx(tx.Hash, prevOutputTxKeys.MoveToImmutable()));
            }
        }

        private void Unmint(IChainStateCursor chainStateCursor, Transaction tx, ChainedHeader chainedHeader, bool isCoinbase)
        {
            // ignore duplicate coinbases
            if ((chainedHeader.Height == DUPE_COINBASE_1_HEIGHT && tx.Hash == DUPE_COINBASE_1_HASH)
                || (chainedHeader.Height == DUPE_COINBASE_2_HEIGHT && tx.Hash == DUPE_COINBASE_2_HASH))
            {
                return;
            }

            // check that transaction exists
            UnspentTx unspentTx;
            if (!chainStateCursor.TryGetUnspentTx(tx.Hash, out unspentTx))
            {
                // missing transaction output
                logger.Warn("Missing transaction at block {0:#,##0}, {1}, tx {2}".Format2(chainedHeader.Height, chainedHeader.Hash.ToHexNumberString(), tx.Hash));
                throw new ValidationException(chainedHeader.Hash);
            }

            //TODO verify blockheight

            // verify all outputs are unspent before unminting
            if (!unspentTx.OutputStates.All(x => x == OutputState.Unspent))
            {
                throw new ValidationException(chainedHeader.Hash);
            }

            // remove the transaction
            if (!chainStateCursor.TryRemoveUnspentTx(tx.Hash))
            {
                throw new ValidationException(chainedHeader.Hash);
            }
        }

        private UnspentTx Unspend(IChainStateCursor chainStateCursor, TxInput input, ChainedHeader chainedHeader)
        {
            bool wasRestored;

            UnspentTx unspentTx;
            if (chainStateCursor.TryGetUnspentTx(input.PreviousTxOutputKey.TxHash, out unspentTx))
            {
                wasRestored = false;
            }
            else
            {
                // unable to rollback, the unspent tx with the block tx key has been pruned
                //TODO better exception
                throw new InvalidOperationException();
            }

            // retrieve previous output index
            var outputIndex = unchecked((int)input.PreviousTxOutputKey.TxOutputIndex);
            if (outputIndex < 0 || outputIndex >= unspentTx.OutputStates.Length)
                throw new Exception("TODO - corruption");

            // check that output isn't already considered unspent
            if (unspentTx.OutputStates[outputIndex] == OutputState.Unspent)
                throw new ValidationException(chainedHeader.Hash);

            // mark output as unspent
            unspentTx = unspentTx.SetOutputState(outputIndex, OutputState.Unspent);

            // increment unspent output count
            chainStateCursor.UnspentOutputCount++;

            // update storage
            if (!wasRestored)
            {
                var wasUpdated = chainStateCursor.TryUpdateUnspentTx(unspentTx);
                if (!wasUpdated)
                    throw new ValidationException(chainedHeader.Hash);
            }
            else
            {
                // a restored fully spent transaction must be added back
                var wasAdded = chainStateCursor.TryAddUnspentTx(unspentTx);
                if (!wasAdded)
                    throw new ValidationException(chainedHeader.Hash);

                // increment unspent tx count
                chainStateCursor.UnspentTxCount++;
            }

            return unspentTx;
        }
    }
}