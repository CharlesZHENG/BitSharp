using System;
using System.Collections.Immutable;
using System.Data.Linq;
using System.Linq;

namespace BitSharp.Core.Domain
{
    public class TxOutput
    {
        public TxOutput(UInt64 value, ImmutableArray<byte> scriptPublicKey)
        {
            Value = value;
            ScriptPublicKey = scriptPublicKey;
        }

        public UInt64 Value { get; }

        public ImmutableArray<byte> ScriptPublicKey { get; }

        public TxOutput With(UInt64? Value = null, ImmutableArray<byte>? ScriptPublicKey = null)
        {
            return new TxOutput
            (
                Value ?? this.Value,
                ScriptPublicKey ?? this.ScriptPublicKey
            );
        }

        public override bool Equals(object obj)
        {
            if (!(obj is TxOutput))
                return false;

            var other = (TxOutput)obj;
            return other.Value == this.Value && other.ScriptPublicKey.SequenceEqual(this.ScriptPublicKey);
        }

        public override int GetHashCode()
        {
            return this.Value.GetHashCode() ^ new Binary(ScriptPublicKey.ToArray()).GetHashCode();
        }
    }
}
