using BitSharp.Core.Script;
using System;
using System.Collections.Immutable;
using System.Data.Linq;
using System.Linq;

namespace BitSharp.Core.Domain
{
    public class PrevTxOutput
    {
        public PrevTxOutput(TxOutput txOutput, int blockHeight, int txIndex, uint txVersion, bool isCoinbase)
            : this(txOutput.Value, txOutput.ScriptPublicKey, blockHeight, txIndex, txVersion, isCoinbase)
        { }

        public PrevTxOutput(UInt64 value, ImmutableArray<byte> scriptPublicKey, int blockHeight, int txIndex, uint txVersion, bool isCoinbase)
        {
            Value = value;
            ScriptPublicKey = scriptPublicKey;
            BlockHeight = blockHeight;
            TxIndex = txIndex;
            TxVersion = txVersion;
            IsCoinbase = isCoinbase;
        }

        public UInt64 Value { get; }

        public ImmutableArray<byte> ScriptPublicKey { get; }

        public int BlockHeight { get; }

        public int TxIndex { get; }

        public uint TxVersion { get; }

        public bool IsCoinbase { get; }

        //TODO have a script class for ScriptPublicKey and ScriptSignature to use
        public bool IsPayToScriptHash()
        {
            return ScriptPublicKey.Length == 23
                && ScriptPublicKey[0] == (byte)ScriptOp.OP_HASH160
                && ScriptPublicKey[1] == 0x14 // push 20 bytes
                && ScriptPublicKey[22] == (byte)ScriptOp.OP_EQUAL;
        }

        public override bool Equals(object obj)
        {
            if (!(obj is PrevTxOutput))
                return false;

            var other = (PrevTxOutput)obj;
            return other.Value == this.Value && other.ScriptPublicKey.SequenceEqual(this.ScriptPublicKey) && other.BlockHeight == this.BlockHeight && other.TxIndex == this.TxIndex && other.TxVersion == this.TxVersion;
        }

        public override int GetHashCode()
        {
            return this.Value.GetHashCode() ^ new Binary(ScriptPublicKey.ToArray()).GetHashCode() ^ this.BlockHeight.GetHashCode() ^ this.TxIndex.GetHashCode() ^ this.TxVersion.GetHashCode();
        }

        public static explicit operator TxOutput(PrevTxOutput prevTxOutput)
        {
            return new TxOutput(prevTxOutput.Value, prevTxOutput.ScriptPublicKey);
        }
    }
}
