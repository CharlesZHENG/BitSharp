﻿using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Data;
using BitSharp.Script;
using BitSharp.Storage;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Blockchain
{
    public class MainnetRules : IBlockchainRules
    {
        public static bool BypassValidation { get; set; }

        public static bool IgnoreScriptErrors { get; set; }

        private const UInt64 SATOSHI_PER_BTC = 100 * 1000 * 1000;

        private readonly BlockHeaderCache blockHeaderCache;

        private readonly UInt256 highestTarget;
        private readonly Block genesisBlock;
        private readonly ChainedBlock genesisChainedBlock;
        private readonly int difficultyInternal = 2016;
        private readonly long difficultyTargetTimespan = 14 * 24 * 60 * 60;

        public MainnetRules(BlockHeaderCache blockHeaderCache)
        {
            this.blockHeaderCache = blockHeaderCache;

            this.highestTarget = UInt256.Parse("00000000FFFF0000000000000000000000000000000000000000000000000000", NumberStyles.HexNumber);

            this.genesisBlock =
                new Block
                (
                    header: new BlockHeader
                    (
                        version: 1,
                        previousBlock: 0,
                        merkleRoot: UInt256.Parse("4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b", NumberStyles.HexNumber),
                        time: 1231006505,
                        bits: 0x1D00FFFF,
                        nonce: 2083236893
                    ),
                    transactions: ImmutableArray.Create
                    (
                        new Transaction
                        (
                            version: 1,
                            inputs: ImmutableArray.Create
                            (
                                new TxInput
                                (
                                    previousTxOutputKey: new TxOutputKey
                                    (
                                        txHash: 0,
                                        txOutputIndex: 0xFFFFFFFF
                                    ),
                                    scriptSignature: ImmutableArray.Create<byte>
                                    (
                                        0x04, 0xFF, 0xFF, 0x00, 0x1D, 0x01, 0x04, 0x45, 0x54, 0x68, 0x65, 0x20, 0x54, 0x69, 0x6D, 0x65,
                                        0x73, 0x20, 0x30, 0x33, 0x2F, 0x4A, 0x61, 0x6E, 0x2F, 0x32, 0x30, 0x30, 0x39, 0x20, 0x43, 0x68,
                                        0x61, 0x6E, 0x63, 0x65, 0x6C, 0x6C, 0x6F, 0x72, 0x20, 0x6F, 0x6E, 0x20, 0x62, 0x72, 0x69, 0x6E,
                                        0x6B, 0x20, 0x6F, 0x66, 0x20, 0x73, 0x65, 0x63, 0x6F, 0x6E, 0x64, 0x20, 0x62, 0x61, 0x69, 0x6C,
                                        0x6F, 0x75, 0x74, 0x20, 0x66, 0x6F, 0x72, 0x20, 0x62, 0x61, 0x6E, 0x6B, 0x73
                                    ),
                                    sequence: 0xFFFFFFFF
                                )
                            ),
                            outputs: ImmutableArray.Create
                            (
                                new TxOutput
                                (
                                    value: 50 * SATOSHI_PER_BTC,
                                    scriptPublicKey: ImmutableArray.Create<byte>
                                    (
                                        0x41, 0x04, 0x67, 0x8A, 0xFD, 0xB0, 0xFE, 0x55, 0x48, 0x27, 0x19, 0x67, 0xF1, 0xA6, 0x71, 0x30,
                                        0xB7, 0x10, 0x5C, 0xD6, 0xA8, 0x28, 0xE0, 0x39, 0x09, 0xA6, 0x79, 0x62, 0xE0, 0xEA, 0x1F, 0x61,
                                        0xDE, 0xB6, 0x49, 0xF6, 0xBC, 0x3F, 0x4C, 0xEF, 0x38, 0xC4, 0xF3, 0x55, 0x04, 0xE5, 0x1E, 0xC1,
                                        0x12, 0xDE, 0x5C, 0x38, 0x4D, 0xF7, 0xBA, 0x0B, 0x8D, 0x57, 0x8A, 0x4C, 0x70, 0x2B, 0x6B, 0xF1,
                                        0x1D, 0x5F, 0xAC
                                    )
                                )
                            ),
                            lockTime: 0
                        )
                    )
                );

            this.genesisChainedBlock = ChainedBlock.CreateForGenesisBlock(this.genesisBlock.Header);
        }

        public virtual UInt256 HighestTarget { get { return this.highestTarget; } }

        public virtual Block GenesisBlock { get { return this.genesisBlock; } }

        public virtual ChainedBlock GenesisChainedBlock { get { return this.genesisChainedBlock; } }

        public virtual int DifficultyInternal { get { return this.difficultyInternal; } }

        public virtual long DifficultyTargetTimespan { get { return this.difficultyTargetTimespan; } }

        public virtual double TargetToDifficulty(UInt256 target)
        {
            // difficulty is HighestTarget / target
            // since these are 256-bit numbers, use division trick for BigIntegers
            return Math.Exp(BigInteger.Log(HighestTarget.ToBigInteger()) - BigInteger.Log(target.ToBigInteger()));
        }

        public virtual UInt256 DifficultyToTarget(double difficulty)
        {
            // implementation is equivalent of HighestTarget / difficulty

            // multiply difficulty and HighestTarget by a scale so that the decimal portion can be fed into a BigInteger
            var scale = 0x100000000L;
            var highestTargetScaled = (BigInteger)HighestTarget * scale;
            var difficultyScaled = (BigInteger)(difficulty * scale);

            // do the division
            var target = highestTargetScaled / difficultyScaled;

            // get the resulting target bytes, taking only the 3 most significant
            var targetBytes = target.ToByteArray();
            targetBytes = new byte[targetBytes.Length - 3].Concat(targetBytes.Skip(targetBytes.Length - 3).ToArray());

            // return the target
            return new UInt256(targetBytes);
        }

        public virtual UInt256 GetRequiredNextTarget(Chain chain)
        {
            try
            {
                // genesis block, use its target
                if (chain.Height == 0)
                {
                    // lookup genesis block header
                    var genesisBlockHeader = this.blockHeaderCache[chain.Blocks[0].BlockHash];

                    return genesisBlockHeader.CalculateTarget();
                }
                // not on an adjustment interval, use previous block's target
                else if (chain.Height % DifficultyInternal != 0)
                {
                    // lookup the previous block on the current blockchain
                    var prevBlockHeader = this.blockHeaderCache[chain.LastBlock.PreviousBlockHash];

                    return prevBlockHeader.CalculateTarget();
                }
                // on an adjustment interval, calculate the required next target
                else
                {
                    // lookup the previous block on the current blockchain
                    var prevBlockHeader = this.blockHeaderCache[chain.LastBlock.PreviousBlockHash];

                    // get the block difficultyInterval blocks ago
                    var startChainedBlock = chain.Blocks.Reverse().Skip(DifficultyInternal).First();
                    var startBlockHeader = this.blockHeaderCache[startChainedBlock.BlockHash];
                    //Debug.Assert(startChainedBlock.Height == blockchain.Height - DifficultyInternal);

                    var actualTimespan = (long)prevBlockHeader.Time - (long)startBlockHeader.Time;
                    var targetTimespan = DifficultyTargetTimespan;

                    // limit adjustment to 4x or 1/4x
                    if (actualTimespan < targetTimespan / 4)
                        actualTimespan = targetTimespan / 4;
                    else if (actualTimespan > targetTimespan * 4)
                        actualTimespan = targetTimespan * 4;

                    // calculate the new target
                    var target = startBlockHeader.CalculateTarget();
                    target *= actualTimespan;
                    target /= targetTimespan;

                    // make sure target isn't too high (too low difficulty)
                    if (target > HighestTarget)
                        target = HighestTarget;

                    return target;
                }
            }
            catch (ArgumentException)
            {
                // invalid bits
                Debugger.Break();
                throw new ValidationException(chain.LastBlock.BlockHash);
            }
        }

        public virtual void ValidateBlock(Block block, ChainStateBuilder chainStateBuilder/*, ImmutableDictionary<UInt256, Transaction> prevInputTxes = null*/)
        {
            //TODO
            if (BypassValidation)
                return;

            // calculate the next required target
            var requiredTarget = GetRequiredNextTarget(chainStateBuilder.Chain.ToImmutable());

            // validate block's target against the required target
            var blockTarget = block.Header.CalculateTarget();
            if (blockTarget > requiredTarget)
            {
                throw new ValidationException(block.Hash, "Failing block {0} at height {1}: Block target {2} did not match required target of {3}".Format2(block.Hash.ToHexNumberString(), chainStateBuilder.Height, blockTarget.ToHexNumberString(), requiredTarget.ToHexNumberString()));
            }

            // validate block's proof of work against its stated target
            if (block.Hash > blockTarget || block.Hash > requiredTarget)
            {
                throw new ValidationException(block.Hash, "Failing block {0} at height {1}: Block did not match its own target of {2}".Format2(block.Hash.ToHexNumberString(), chainStateBuilder.Height, blockTarget.ToHexNumberString()));
            }

            // ensure there is at least 1 transaction
            if (block.Transactions.Count == 0)
            {
                throw new ValidationException(block.Hash, "Failing block {0} at height {1}: Zero transactions present".Format2(block.Hash.ToHexNumberString(), chainStateBuilder.Height));
            }

            //TODO apply real coinbase rule
            // https://github.com/bitcoin/bitcoin/blob/481d89979457d69da07edd99fba451fd42a47f5c/src/core.h#L219
            var coinbaseTx = block.Transactions[0];

            // check that coinbase has only one input
            if (coinbaseTx.Inputs.Count != 1)
            {
                throw new ValidationException(block.Hash, "Failing block {0} at height {1}: Coinbase transaction does not have exactly one input".Format2(block.Hash.ToHexNumberString(), chainStateBuilder.Height));
            }

            var blockTxIndices = new Dictionary<UInt256, int>();
            for (var i = 0; i < block.Transactions.Count; i++)
                blockTxIndices.Add(block.Transactions[i].Hash, i);

            // validate transactions in parallel
            long unspentValue = 0L;
            try
            {
                Parallel.For(1, block.Transactions.Count, (txIndex, loopState) =>
                {
                    var tx = block.Transactions[txIndex];

                    long unspentValueInner;
                    //TODO utxo will not be correct at transaction if a tx hash is reused within the same block
                    ValidateTransaction(chainStateBuilder.Height, block, tx, txIndex, chainStateBuilder.Utxo, out unspentValueInner, blockTxIndices/*, prevInputTxes*/);

                    Interlocked.Add(ref unspentValue, unspentValueInner);
                });
            }
            catch (AggregateException e)
            {
                var validationException = e.InnerExceptions.FirstOrDefault(x => x is ValidationException);
                var missingDataException = e.InnerExceptions.FirstOrDefault(x => x is MissingDataException);

                if (validationException != null)
                    throw validationException;

                if (missingDataException != null)
                    throw missingDataException;

                throw;
            }

            //TODO utxo will not be correct at transaction if a tx hash is reused within the same block
            ValidateTransactionScripts(block, chainStateBuilder.Utxo, blockTxIndices/*, prevInputTxes*/);

            // calculate the expected reward in coinbase
            var expectedReward = (long)(50 * SATOSHI_PER_BTC);
            if (chainStateBuilder.Height / 210000 <= 32)
                expectedReward /= (long)Math.Pow(2, chainStateBuilder.Height / 210000);
            expectedReward += unspentValue;

            // calculate the actual reward in coinbase
            var actualReward = 0L;
            foreach (var txOutput in coinbaseTx.Outputs)
                actualReward += (long)txOutput.Value;

            // ensure coinbase has correct reward
            if (actualReward > expectedReward)
            {
                throw new ValidationException(block.Hash, "Failing block {0} at height {1}: Coinbase value is greater than reward + fees".Format2(block.Hash.ToHexNumberString(), chainStateBuilder.Height));
            }

            // all validation has passed
        }

        //TODO utxo needs to be as-at transaction, with regards to a transaction being fully spent and added back in in the same block
        public virtual void ValidateTransaction(int blockHeight, Block block, Transaction tx, int txIndex, UtxoBuilder utxoBuilder, out long unspentValue, Dictionary<UInt256, int> blockTxIndices/*, ImmutableDictionary<UInt256, Transaction> prevInputTxes = null*/)
        {
            unspentValue = -1;

            // lookup all previous outputs
            var prevOutputMissing = false;
            var previousOutputs = new Dictionary<TxOutputKey, Tuple<TxInput, int, TxOutput>>();
            for (var inputIndex = 0; inputIndex < tx.Inputs.Count; inputIndex++)
            {
                var input = tx.Inputs[inputIndex];

                // find previous transaction
                //var prevTx = prevInputTxes != null ? prevInputTxes[input.PreviousTxOutputKey.TxHash]
                //    : this.CacheContext.TransactionCache[input.PreviousTxOutputKey.TxHash];

                // find previous transaction output
                //if (input.PreviousTxOutputKey.TxOutputIndex >= prevTx.Outputs.Count)
                //    throw new ValidationException(block.Hash);
                //var prevOutput = prevTx.Outputs[input.PreviousTxOutputKey.TxOutputIndex.ToIntChecked()];
                var prevOutput = LookupPreviousOutput(input.PreviousTxOutputKey, block, blockTxIndices, utxoBuilder);

                previousOutputs.Add(input.PreviousTxOutputKey, Tuple.Create(input, inputIndex, prevOutput));
            }

            if (prevOutputMissing)
            {
                throw new ValidationException(block.Hash);
            }

            // verify spend amounts
            var txInputValue = (UInt64)0;
            var txOutputValue = (UInt64)0;

            for (var inputIndex = 0; inputIndex < tx.Inputs.Count; inputIndex++)
            {
                var input = tx.Inputs[inputIndex];

                // add transactions previous value to unspent amount (used to calculate allowed coinbase reward)
                var prevOutput = previousOutputs[input.PreviousTxOutputKey].Item3;
                txInputValue += prevOutput.Value;
            }

            for (var outputIndex = 0; outputIndex < tx.Outputs.Count; outputIndex++)
            {
                // remove transactions spend value from unspent amount (used to calculate allowed coinbase reward)
                var output = tx.Outputs[outputIndex];
                txOutputValue += output.Value;
            }

            // ensure that amount being output from transaction isn't greater than amount being input
            if (txOutputValue > txInputValue)
            {
                throw new ValidationException(block.Hash, "Failing tx {0}: Transaction output value is greater than input value".Format2(tx.Hash.ToHexNumberString()));
            }

            // calculate unspent value
            unspentValue = (long)(txInputValue - txOutputValue);

            // sanity check
            if (unspentValue < 0)
            {
                throw new ValidationException(block.Hash);
            }

            // all validation has passed
        }

        //TODO utxo needs to be as-at transaction, with regards to a transaction being fully spent and added back in in the same block
        public virtual void ValidateTransactionScripts(Block block, UtxoBuilder utxoBuilder, Dictionary<UInt256, int> blockTxIndices/*, ImmutableDictionary<UInt256, Transaction> prevInputTxes = null*/)
        {
            var exceptions = new ConcurrentBag<Exception>();

            var scriptEngine = new ScriptEngine();

            Parallel.ForEach(GetAllInputsIndexed(block, blockTxIndices, utxoBuilder), (tuple, loopState) =>
            {
                try
                {
                    var tx = tuple.Item1;
                    var input = tuple.Item2;
                    var inputIndex = tuple.Item3;
                    var prevOutput = tuple.Item4;

                    // create the transaction script from the input and output
                    var script = input.ScriptSignature.AddRange(prevOutput.ScriptPublicKey);
                    if (!scriptEngine.VerifyScript(0 /*TODO blockHash*/, 0 /*TODO txIndex*/, prevOutput.ScriptPublicKey.ToArray(), tx, inputIndex, script.ToArray()))
                    {
                        exceptions.Add(new ValidationException(block.Hash));
                        if (!IgnoreScriptErrors)
                            loopState.Break();
                    }
                }
                catch (Exception e)
                {
                    exceptions.Add(e);
                    if (!IgnoreScriptErrors)
                        loopState.Break();
                }
            });

            if (exceptions.Count > 0)
            {
                if (!IgnoreScriptErrors)
                    throw new AggregateException(exceptions.ToArray());
                //else
                //    Debug.WriteLine("Ignoring script error in block: {0}".Format2(block.Hash));
            }
        }

        private IEnumerable<Tuple<Transaction, TxInput, int, TxOutput>> GetAllInputsIndexed(Block block, Dictionary<UInt256, int> blockTxIndices, UtxoBuilder utxoBuilder)
        {
            for (var txIndex = 1; txIndex < block.Transactions.Count; txIndex++)
            {
                var tx = block.Transactions[txIndex];

                for (var inputIndex = 0; inputIndex < tx.Inputs.Count; inputIndex++)
                {
                    var input = tx.Inputs[inputIndex];
                    var prevOutput = LookupPreviousOutput(input.PreviousTxOutputKey, block, blockTxIndices, utxoBuilder);

                    yield return Tuple.Create(tx, input, inputIndex, prevOutput);
                }
            }
        }

        private TxOutput LookupPreviousOutput(TxOutputKey txOutputKey, Block block, Dictionary<UInt256, int> blockTxIndices, UtxoBuilder utxoBuilder)
        {
            TxOutput prevOutput;
            if (utxoBuilder.TryGetOutput(txOutputKey, out prevOutput))
            {
                return prevOutput;
            }
            else
            {
                Transaction prevTx;
                int prevTxIndex;
                if (blockTxIndices.TryGetValue(txOutputKey.TxHash, out prevTxIndex))
                {
                    Debug.Assert(prevTxIndex >= 0 && prevTxIndex < block.Transactions.Count);
                    prevTx = block.Transactions[prevTxIndex];
                    Debug.Assert(prevTx.Hash == txOutputKey.TxHash);
                }
                else
                {
                    throw new ValidationException(block.Hash);
                }

                var outputIndex = unchecked((int)txOutputKey.TxOutputIndex);
                if (outputIndex < 0 || outputIndex >= prevTx.Outputs.Count)
                    throw new ValidationException(block.Hash);

                return prevTx.Outputs[outputIndex];
            }
        }
    }
}
