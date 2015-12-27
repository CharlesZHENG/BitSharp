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
            InputTxes = new CompletionArray<DecodedTx>(txIndex != 0 ? transaction.Inputs.Length : 0);
        }

        public bool IsCoinbase => Transaction.IsCoinbase;

        public Transaction Transaction { get; }

        public int TxIndex { get; }

        public ChainedHeader ChainedHeader { get; }

        public ImmutableArray<TxLookupKey> PrevOutputTxKeys { get; }

        public CompletionArray<DecodedTx> InputTxes { get; }

        public LoadedTx ToLoadedTx()
        {
            return new LoadedTx(this.Transaction, this.TxIndex, this.InputTxes.CompletedArray);
        }
    }
}
