using BitSharp.Common;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Core.Domain
{
    public class UnmintedTx
    {
        public UnmintedTx(UInt256 txHash, ImmutableArray<PrevTxOutput> prevTxOutputs)
        {
            TxHash = txHash;
            PrevTxOutputs = prevTxOutputs;
        }

        public UInt256 TxHash { get; }

        public ImmutableArray<PrevTxOutput> PrevTxOutputs { get; }

        public override bool Equals(object obj)
        {
            if (!(obj is UnmintedTx))
                return false;

            var other = (UnmintedTx)obj;
            return other.TxHash == this.TxHash && other.PrevTxOutputs.SequenceEqual(this.PrevTxOutputs);
        }

        public override int GetHashCode()
        {
            return this.TxHash.GetHashCode(); //TODO ^ this.PrevTxOutputs.GetHashCode();
        }
    }
}
