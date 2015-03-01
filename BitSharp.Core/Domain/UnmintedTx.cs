using BitSharp.Common;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Core.Domain
{
    public class UnmintedTx
    {
        private readonly UInt256 txHash;
        private readonly ImmutableArray<BlockTxKey> prevOutputTxKeys;

        public UnmintedTx(UInt256 txHash, ImmutableArray<BlockTxKey> prevOutputTxKeys)
        {
            this.txHash = txHash;
            this.prevOutputTxKeys = prevOutputTxKeys;
        }

        public UInt256 TxHash { get { return this.txHash; } }

        public ImmutableArray<BlockTxKey> PrevOutputTxKeys { get { return this.prevOutputTxKeys; } }

        public override bool Equals(object obj)
        {
            if (!(obj is UnmintedTx))
                return false;

            var other = (UnmintedTx)obj;
            return other.txHash == this.txHash && other.prevOutputTxKeys.SequenceEqual(this.prevOutputTxKeys);
        }

        public override int GetHashCode()
        {
            return this.txHash.GetHashCode(); //TODO ^ this.prevOutputTxKeys.GetHashCode();
        }
    }
}
