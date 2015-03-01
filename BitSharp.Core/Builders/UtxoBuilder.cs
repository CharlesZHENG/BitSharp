using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage;
using NLog;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Globalization;
using System.Linq;

namespace BitSharp.Core.Builders
{
    internal class UtxoBuilder
    {
        private static readonly int DUPE_COINBASE_1_HEIGHT = 91722;
        private static readonly UInt256 DUPE_COINBASE_1_HASH = UInt256.Parse("e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468", NumberStyles.HexNumber);
        private static readonly int DUPE_COINBASE_2_HEIGHT = 91812;
        private static readonly UInt256 DUPE_COINBASE_2_HASH = UInt256.Parse("d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599", NumberStyles.HexNumber);

        private readonly Logger logger;

        private readonly IChainStateCursor chainStateCursor;

        public UtxoBuilder(IChainStateCursor chainStateCursor, Logger logger)
        {
            this.logger = logger;

            this.chainStateCursor = chainStateCursor;
        }

        public IEnumerable<TxWithPrevOutputKeys> CalculateUtxo(Chain chain, IEnumerable<Transaction> blockTxes)
        {
            var blockSpentTxes = ImmutableList.CreateBuilder<SpentTx>();

            var chainedHeader = chain.LastBlock;

            var txIndex = -1;
            foreach (var tx in blockTxes)
            {
                txIndex++;

                // there exist two duplicate coinbases in the blockchain, which the design assumes to be impossible
                // ignore the first occurrences of these duplicates so that they do not need to later be deleted from the utxo, an unsupported operation
                // no other duplicates will occur again, it is now disallowed
                if ((chainedHeader.Height == DUPE_COINBASE_1_HEIGHT && tx.Hash == DUPE_COINBASE_1_HASH)
                    || (chainedHeader.Height == DUPE_COINBASE_2_HEIGHT && tx.Hash == DUPE_COINBASE_2_HASH))
                {
                    continue;
                }

                var prevOutputTxKeys = ImmutableArray.CreateBuilder<BlockTxKey>(tx.Inputs.Length);

                //TODO apply real coinbase rule
                // https://github.com/bitcoin/bitcoin/blob/481d89979457d69da07edd99fba451fd42a47f5c/src/core.h#L219
                if (txIndex > 0)
                {
                    // spend each of the transaction's inputs in the utxo
                    for (var inputIndex = 0; inputIndex < tx.Inputs.Length; inputIndex++)
                    {
                        var input = tx.Inputs[inputIndex];
                        var unspentTx = this.Spend(txIndex, tx, inputIndex, input, chainedHeader, blockSpentTxes);

                        var unspentTxBlockHash = chain.Blocks[unspentTx.BlockIndex].Hash;
                        prevOutputTxKeys.Add(new BlockTxKey(unspentTxBlockHash, unspentTx.TxIndex));
                    }
                }

                // mint the transaction's outputs in the utxo
                this.Mint(tx, txIndex, chainedHeader);

                // increment unspent tx count
                this.chainStateCursor.UnspentTxCount++;

                yield return new TxWithPrevOutputKeys(txIndex, tx, chainedHeader, prevOutputTxKeys.ToImmutable());
            }

            if (!this.chainStateCursor.TryAddBlockSpentTxes(chainedHeader.Height, blockSpentTxes.ToImmutable()))
                throw new ValidationException(chainedHeader.Height);
        }

        private void Mint(Transaction tx, int txIndex, ChainedHeader chainedHeader)
        {
            // add transaction to the utxo
            var unspentTx = new UnspentTx(tx.Hash, chainedHeader.Height, txIndex, tx.Outputs.Length, OutputState.Unspent);
            if (!this.chainStateCursor.TryAddUnspentTx(unspentTx))
            {
                // duplicate transaction
                this.logger.Warn("Duplicate transaction at block {0:#,##0}, {1}, coinbase".Format2(chainedHeader.Height, chainedHeader.Hash.ToHexNumberString()));
                throw new ValidationException(chainedHeader.Hash);
            }
        }

        private UnspentTx Spend(int txIndex, Transaction tx, int inputIndex, TxInput input, ChainedHeader chainedHeader, ImmutableList<SpentTx>.Builder blockSpentTxes)
        {
            UnspentTx unspentTx;
            if (!this.chainStateCursor.TryGetUnspentTx(input.PreviousTxOutputKey.TxHash, out unspentTx))
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

            // update transaction output states in the utxo
            var wasUpdated = this.chainStateCursor.TryUpdateUnspentTx(unspentTx);
            if (!wasUpdated)
                throw new ValidationException(chainedHeader.Hash);

            // store pruning information for a fully spent transaction
            if (unspentTx.IsFullySpent)
            {
                blockSpentTxes.Add(unspentTx.ToSpentTx(chainedHeader.Height));

                // decrement unspent tx count
                this.chainStateCursor.UnspentTxCount--;
            }

            return unspentTx;
        }

        //TODO with the rollback information that's now being stored, rollback could be down without needing the block
        public void RollbackUtxo(Chain chain, ChainedHeader chainedHeader, IEnumerable<BlockTx> blockTxes, ImmutableDictionary<UInt256, SpentTx> spentTxes, ImmutableList<UnmintedTx>.Builder unmintedTxes)
        {
            //TODO don't reverse here, storage should be read in reverse
            foreach (var blockTx in blockTxes.Reverse())
            {
                var tx = blockTx.Transaction;
                var txIndex = blockTx.Index;

                // remove transaction outputs
                this.Unmint(tx, chainedHeader, isCoinbase: true);

                // decrement unspent tx count
                this.chainStateCursor.UnspentTxCount--;

                var prevOutputTxKeys = ImmutableArray.CreateBuilder<BlockTxKey>(tx.Inputs.Length);

                if (txIndex > 0)
                {
                    // remove inputs in reverse order
                    for (var inputIndex = tx.Inputs.Length - 1; inputIndex >= 0; inputIndex--)
                    {
                        var input = tx.Inputs[inputIndex];
                        var unspentTx = this.Unspend(input, chainedHeader, spentTxes);

                        // store rollback replay information
                        var unspentTxBlockHash = chain.Blocks[unspentTx.BlockIndex].Hash;
                        prevOutputTxKeys.Add(new BlockTxKey(unspentTxBlockHash, unspentTx.TxIndex));
                    }
                }

                // store rollback replay information
                unmintedTxes.Add(new UnmintedTx(tx.Hash, prevOutputTxKeys.ToImmutable()));
            }
        }

        private void Unmint(Transaction tx, ChainedHeader chainedHeader, bool isCoinbase)
        {
            // ignore duplicate coinbases
            if ((chainedHeader.Height == DUPE_COINBASE_1_HEIGHT && tx.Hash == DUPE_COINBASE_1_HASH)
                || (chainedHeader.Height == DUPE_COINBASE_2_HEIGHT && tx.Hash == DUPE_COINBASE_2_HASH))
            {
                return;
            }

            // check that transaction exists
            UnspentTx unspentTx;
            if (!this.chainStateCursor.TryGetUnspentTx(tx.Hash, out unspentTx))
            {
                // missing transaction output
                this.logger.Warn("Missing transaction at block {0:#,##0}, {1}, tx {2}".Format2(chainedHeader.Height, chainedHeader.Hash.ToHexNumberString(), tx.Hash));
                throw new ValidationException(chainedHeader.Hash);
            }

            //TODO verify blockheight

            // verify all outputs are unspent before unminting
            if (!unspentTx.OutputStates.All(x => x == OutputState.Unspent))
            {
                throw new ValidationException(chainedHeader.Hash);
            }

            // remove the transaction
            if (!this.chainStateCursor.TryRemoveUnspentTx(tx.Hash))
            {
                throw new ValidationException(chainedHeader.Hash);
            }
        }

        private UnspentTx Unspend(TxInput input, ChainedHeader chainedHeader, ImmutableDictionary<UInt256, SpentTx> spentTxes)
        {
            bool wasRestored;

            UnspentTx unspentTx;
            if (this.chainStateCursor.TryGetUnspentTx(input.PreviousTxOutputKey.TxHash, out unspentTx))
            {
                wasRestored = false;
            }
            else
            {
                // lookup fully spent transaction
                SpentTx spentTx;
                if (!spentTxes.TryGetValue(input.PreviousTxOutputKey.TxHash, out spentTx))
                    throw new ValidationException(chainedHeader.Hash);

                // restore fully spent transaction
                unspentTx = new UnspentTx(spentTx.TxHash, spentTx.ConfirmedBlockIndex, spentTx.TxIndex, new OutputStates(spentTx.OutputCount, OutputState.Spent));
                wasRestored = true;
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

            // update storage
            if (!wasRestored)
            {
                var wasUpdated = this.chainStateCursor.TryUpdateUnspentTx(unspentTx);
                if (!wasUpdated)
                    throw new ValidationException(chainedHeader.Hash);
            }
            else
            {
                // a restored fully spent transaction must be added back
                var wasAdded = this.chainStateCursor.TryAddUnspentTx(unspentTx);
                if (!wasAdded)
                    throw new ValidationException(chainedHeader.Hash);

                // increment unspent tx count
                this.chainStateCursor.UnspentTxCount--;
            }

            return unspentTx;
        }
    }
}