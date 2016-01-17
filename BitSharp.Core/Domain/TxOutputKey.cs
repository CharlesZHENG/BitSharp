using BitSharp.Common;
using System;

namespace BitSharp.Core.Domain
{
    public class TxOutputKey : IComparable<TxOutputKey>
    {
        private readonly int hashCode;

        public TxOutputKey(UInt256 txHash, UInt32 txOutputIndex)
        {
            TxHash = txHash;
            TxOutputIndex = txOutputIndex;

            this.hashCode = txHash.GetHashCode() ^ txOutputIndex.GetHashCode();
        }

        public UInt256 TxHash { get; }

        public UInt32 TxOutputIndex { get; }

        public override bool Equals(object obj)
        {
            if (!(obj is TxOutputKey))
                return false;

            return (TxOutputKey)obj == this;
        }

        public override int GetHashCode()
        {
            return this.hashCode;
        }

        public int CompareTo(TxOutputKey other)
        {
            var result = TxHash.CompareTo(other.TxHash);
            if (result != 0)
                return result;
            else
                return TxOutputIndex.CompareTo(other.TxOutputIndex);
        }

        public static bool operator ==(TxOutputKey left, TxOutputKey right)
        {
            return object.ReferenceEquals(left, right) || (!object.ReferenceEquals(left, null) && !object.ReferenceEquals(right, null) && left.TxHash == right.TxHash && left.TxOutputIndex == right.TxOutputIndex);
        }

        public static bool operator !=(TxOutputKey left, TxOutputKey right)
        {
            return !(left == right);
        }
    }
}
