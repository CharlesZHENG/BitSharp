using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Core.Builders
{
    internal class LoadingTx
    {
        public LoadingTx(int txIndex, Transaction transaction, ChainedHeader chainedHeader, ImmutableArray<TxLookupKey> prevOutputTxKeys, ImmutableArray<ImmutableArray<byte>>? inputTxesBytes)
        {
            Transaction = transaction;
            TxIndex = txIndex;
            ChainedHeader = chainedHeader;
            PrevOutputTxKeys = prevOutputTxKeys;
            InputTxesBytes = new CompletionArray<ImmutableArray<byte>>(txIndex != 0 ? transaction.Inputs.Length : 0);

            if (inputTxesBytes != null)
            {
                if (txIndex == 0 && inputTxesBytes.Value.Length != 0)
                    throw new ArgumentException(nameof(inputTxesBytes));
                else if (txIndex > 0 && inputTxesBytes.Value.Length != transaction.Inputs.Length)
                    throw new ArgumentException(nameof(inputTxesBytes));

                for (var inputTxIndex = 0; inputTxIndex < inputTxesBytes.Value.Length; inputTxIndex++)
                {
                    var inputTxBytes = inputTxesBytes.Value[inputTxIndex];
                    InputTxesBytes.TryComplete(inputTxIndex, inputTxBytes);

                    //if (inputTx.Hash != transaction.Inputs[inputTxIndex].PreviousTxOutputKey.TxHash)
                    //    throw new InvalidOperationException();

                    //var prevOutputTxIndex = transaction.Inputs[inputTxIndex].PreviousTxOutputKey.TxOutputIndex.ToIntChecked();
                    //if (prevOutputTxIndex < 0 || prevOutputTxIndex >= inputTx.Outputs.Length)
                    //    throw new InvalidOperationException();
                }

                if (!InputTxesBytes.IsComplete)
                    throw new InvalidOperationException();

                IsPreLoaded = true;
            }
            else
                IsPreLoaded = false;
        }

        public bool IsCoinbase => this.TxIndex == 0;

        public Transaction Transaction { get; }

        public int TxIndex { get; }

        public ChainedHeader ChainedHeader { get; }

        public ImmutableArray<TxLookupKey> PrevOutputTxKeys { get; }

        public CompletionArray<ImmutableArray<byte>> InputTxesBytes { get; }

        public bool IsPreLoaded { get; }

        public LoadedTx ToLoadedTx()
        {
            var inputTxes = this.InputTxesBytes.CompletedArray.Select(x => DataEncoder.DecodeTransaction(x.ToArray())).ToImmutableArray();

            return new LoadedTx(this.Transaction, this.TxIndex, inputTxes);
        }
    }
}
