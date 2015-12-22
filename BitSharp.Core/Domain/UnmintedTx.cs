using BitSharp.Common;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Core.Domain
{
    public class UnmintedTx
    {
        public UnmintedTx(UInt256 txHash, ImmutableArray<TxLookupKey> prevOutputTxKeys, ImmutableArray<ImmutableArray<byte>>? inputTxesBytes)
        {
            TxHash = txHash;
            PrevOutputTxKeys = prevOutputTxKeys;
            InputTxesBytes = inputTxesBytes;
        }

        public UInt256 TxHash { get; }

        public ImmutableArray<TxLookupKey> PrevOutputTxKeys { get; }

        public ImmutableArray<ImmutableArray<byte>>? InputTxesBytes { get; }

        public override bool Equals(object obj)
        {
            if (!(obj is UnmintedTx))
                return false;

            var other = (UnmintedTx)obj;
            return other.TxHash == this.TxHash && other.PrevOutputTxKeys.SequenceEqual(this.PrevOutputTxKeys);
        }

        public override int GetHashCode()
        {
            return this.TxHash.GetHashCode(); //TODO ^ this.prevOutputTxKeys.GetHashCode();
        }
    }
}
