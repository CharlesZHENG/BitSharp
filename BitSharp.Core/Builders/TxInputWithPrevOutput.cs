using BitSharp.Core.Domain;

namespace BitSharp.Core.Builders
{
    public class TxInputWithPrevOutput
    {
        private readonly ChainedHeader chainedHeader;
        private readonly Transaction transaction;
        private readonly int txIndex;
        private readonly TxInput txInput;
        private readonly int inputIndex;
        private readonly TxOutput prevTxOutput;

        public TxInputWithPrevOutput(ChainedHeader chainedHeader, Transaction transaction, int txIndex, TxInput txInput, int inputIndex, TxOutput prevTxOutput)
        {
            this.chainedHeader = chainedHeader;
            this.transaction = transaction;
            this.txIndex = txIndex;
            this.txInput = txInput;
            this.inputIndex = inputIndex;
            this.prevTxOutput = prevTxOutput;
        }

        public ChainedHeader ChainedHeader { get { return this.chainedHeader; } }

        public Transaction Transaction { get { return this.transaction; } }

        public int TxIndex { get { return this.txIndex; } }

        public TxInput TxInput { get { return this.txInput; } }

        public int InputIndex { get { return this.inputIndex; } }

        public TxOutput PrevTxOutput { get { return this.prevTxOutput; } }
    }
}
