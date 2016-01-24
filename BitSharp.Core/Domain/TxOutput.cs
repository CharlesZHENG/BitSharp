using System;
using System.Collections.Immutable;
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

        //TODO only exists for tests
        public override bool Equals(object obj)
        {
            if (!(obj is TxOutput))
                return false;

            var other = (TxOutput)obj;
            return other.Value == this.Value && other.ScriptPublicKey.SequenceEqual(this.ScriptPublicKey);
        }
    }
}
