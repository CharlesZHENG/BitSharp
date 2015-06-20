using BitSharp.Core.Domain;
using System.Collections.Immutable;

namespace BitSharp.Core.Builders
{
    internal class LoadingTx
    {
        private readonly Transaction transaction;
        private readonly int txIndex;
        private readonly ChainedHeader chainedHeader;
        private readonly ImmutableArray<TxLookupKey> prevOutputTxKeys;
        private readonly CompletionArray<Transaction> inputTxes;

        public LoadingTx(int txIndex, Transaction transaction, ChainedHeader chainedHeader, ImmutableArray<TxLookupKey> prevOutputTxKeys)
        {
            this.transaction = transaction;
            this.txIndex = txIndex;
            this.chainedHeader = chainedHeader;
            this.prevOutputTxKeys = prevOutputTxKeys;
            this.inputTxes = new CompletionArray<Transaction>(transaction.Inputs.Length);
        }

        public bool IsCoinbase { get { return this.txIndex == 0; } }

        public Transaction Transaction { get { return this.transaction; } }

        public int TxIndex { get { return this.txIndex; } }

        public ChainedHeader ChainedHeader { get { return this.chainedHeader; } }

        public ImmutableArray<TxLookupKey> PrevOutputTxKeys { get { return this.prevOutputTxKeys; } }

        public CompletionArray<Transaction> InputTxes { get { return this.inputTxes; } }

        public LoadedTx ToLoadedTx()
        {
            return new LoadedTx(this.transaction, this.txIndex, this.inputTxes.CompletedArray);
        }
    }
}
