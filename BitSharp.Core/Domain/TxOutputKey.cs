using BitSharp.Common;
using System;

namespace BitSharp.Core.Domain
{
    public class TxOutputKey
    {
        private readonly UInt256 _txHash;
        private readonly UInt32 _txOutputIndex;

        private readonly int hashCode;

        public TxOutputKey(UInt256 txHash, UInt32 txOutputIndex)
        {
            this._txHash = txHash;
            this._txOutputIndex = txOutputIndex;

            this.hashCode = txHash.GetHashCode() ^ txOutputIndex.GetHashCode();
        }

        public UInt256 TxHash { get { return this._txHash; } }

        public UInt32 TxOutputIndex { get { return this._txOutputIndex; } }

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
