using BitSharp.Common;

namespace BitSharp.Core.Monitor
{
    public class ChainPosition
    {
        public ChainPosition(UInt256 blockHash, int txIndex, UInt256 txHash, int inputIndex, int outputIndex)
        {
            BlockHash = blockHash;
            TxIndex = txIndex;
            TxHash = txHash;
            InputIndex = inputIndex;
            OutputIndex = outputIndex;
        }

        public UInt256 BlockHash { get; }

        public int TxIndex { get; }

        public UInt256 TxHash { get; }

        public int InputIndex { get; }

        public int OutputIndex { get; }

        //TODO
        public static ChainPosition Fake()
        {
            return new ChainPosition(UInt256.Zero, 0, UInt256.Zero, 0, 0);
        }
    }
}
