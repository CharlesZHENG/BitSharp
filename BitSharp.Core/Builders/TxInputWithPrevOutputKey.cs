using BitSharp.Core.Domain;

namespace BitSharp.Core.Builders
{
    internal class TxInputWithPrevOutputKey
    {
        private readonly Transaction transaction;
        private readonly int txIndex;
        private readonly ChainedHeader chainedHeader;
        private readonly int inputIndex;
        private readonly BlockTxKey prevOutputTxKey;

        public TxInputWithPrevOutputKey(int txIndex, Transaction transaction, ChainedHeader chainedHeader, int inputIndex, BlockTxKey prevOutputTxKey)
        {
            this.transaction = transaction;
            this.txIndex = txIndex;
            this.chainedHeader = chainedHeader;
            this.inputIndex = inputIndex;
            this.prevOutputTxKey = prevOutputTxKey;
        }

        public Transaction Transaction { get { return this.transaction; } }

        public int TxIndex { get { return this.txIndex; } }

        public ChainedHeader ChainedHeader { get { return this.chainedHeader; } }

        public int InputIndex { get { return this.inputIndex; } }

        public BlockTxKey PrevOutputTxKey { get { return this.prevOutputTxKey; } }
    }
}
