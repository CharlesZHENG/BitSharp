using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Core.Builders
{
    internal class LoadingTx
    {
        public LoadingTx(int txIndex, Transaction transaction, ChainedHeader chainedHeader, ImmutableArray<TxLookupKey> prevOutputTxKeys)
        {
            Transaction = transaction;
            TxIndex = txIndex;
            ChainedHeader = chainedHeader;
            PrevOutputTxKeys = prevOutputTxKeys;
            InputTxesBytes = new CompletionArray<ImmutableArray<byte>>(txIndex != 0 ? transaction.Inputs.Length : 0);
        }

        public bool IsCoinbase => this.TxIndex == 0;

        public Transaction Transaction { get; }

        public int TxIndex { get; }

        public ChainedHeader ChainedHeader { get; }

        public ImmutableArray<TxLookupKey> PrevOutputTxKeys { get; }

        public CompletionArray<ImmutableArray<byte>> InputTxesBytes { get; }

        public LoadedTx ToLoadedTx()
        {
            var inputTxes = this.InputTxesBytes.CompletedArray.Select(x => DataEncoder.DecodeTransaction(x.ToArray())).ToImmutableArray();

            return new LoadedTx(this.Transaction, this.TxIndex, inputTxes);
        }
    }
}
