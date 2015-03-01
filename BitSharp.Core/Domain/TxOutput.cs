using System;
using System.Collections.Immutable;
using System.Data.Linq;
using System.Linq;

namespace BitSharp.Core.Domain
{
    public class TxOutput
    {
        private readonly UInt64 _value;
        private readonly ImmutableArray<byte> _scriptPublicKey;

        public TxOutput(UInt64 value, ImmutableArray<byte> scriptPublicKey)
        {
            this._value = value;
            this._scriptPublicKey = scriptPublicKey;
        }

        public UInt64 Value { get { return this._value; } }

        public ImmutableArray<byte> ScriptPublicKey { get { return this._scriptPublicKey; } }

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
            return other._value == this._value && other._scriptPublicKey.SequenceEqual(this._scriptPublicKey);
        }

        public override int GetHashCode()
        {
            return this._value.GetHashCode() ^ new Binary(_scriptPublicKey.ToArray()).GetHashCode();
        }
    }
}
