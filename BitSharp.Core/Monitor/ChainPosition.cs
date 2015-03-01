using BitSharp.Common;

namespace BitSharp.Core.Monitor
{
    public class ChainPosition
    {
        private readonly UInt256 blockHash;
        private readonly int txIndex;
        private readonly UInt256 txHash;
        private readonly int inputIndex;
        private readonly int outputIndex;

        public ChainPosition(UInt256 blockHash, int txIndex, UInt256 txHash, int inputIndex, int outputIndex)
        {
            this.blockHash = blockHash;
            this.txIndex = txIndex;
            this.txHash = txHash;
            this.inputIndex = inputIndex;
            this.outputIndex = outputIndex;
        }

        public UInt256 BlockHash { get { return this.blockHash; } }

        public int TxIndex { get { return this.txIndex; } }

        public UInt256 TxHash { get { return this.txHash; } }

        public int InputIndex { get { return this.inputIndex; } }

        public int OutputIndex { get { return this.outputIndex; } }

        //TODO
        public static ChainPosition Fake()
        {
            return new ChainPosition(0, 0, 0, 0, 0);
        }
    }
}
