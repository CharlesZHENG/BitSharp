using BitSharp.Core.Domain;
using System.Collections.Immutable;

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
            InputTxes = new CompletionArray<Transaction>(transaction.Inputs.Length);
        }

        public bool IsCoinbase => this.TxIndex == 0;

        public Transaction Transaction { get; }

        public int TxIndex { get; }

        public ChainedHeader ChainedHeader { get; }

        public ImmutableArray<TxLookupKey> PrevOutputTxKeys { get; }

        public CompletionArray<Transaction> InputTxes { get; }

        public LoadedTx ToLoadedTx()
        {
            return new LoadedTx(this.Transaction, this.TxIndex, this.InputTxes.CompletedArray);
        }
    }
}
