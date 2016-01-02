using BitSharp.Common;
using System;
using System.Collections.Immutable;

namespace BitSharp.Core.Domain
{
    public class TxInput
    {
        public TxInput(TxOutputKey prevTxOutputKey, ImmutableArray<byte> scriptSignature, UInt32 sequence)
        {
            PrevTxOutputKey = prevTxOutputKey;
            ScriptSignature = scriptSignature;
            Sequence = sequence;
        }

        public TxInput(UInt256 prevTxHash, uint prevTxOutputIndex, ImmutableArray<byte> scriptSignature, UInt32 sequence)
            : this(new TxOutputKey(prevTxHash, prevTxOutputIndex), scriptSignature, sequence)
        { }

        public TxOutputKey PrevTxOutputKey { get; }

        public UInt256 PrevTxHash => PrevTxOutputKey.TxHash;

        public uint PrevTxOutputIndex => PrevTxOutputKey.TxOutputIndex;

        public ImmutableArray<byte> ScriptSignature { get; }

        public UInt32 Sequence { get; }

        public TxInput With(TxOutputKey previousTxOutput = null, ImmutableArray<byte>? scriptSignature = null, UInt32? sequence = null)
        {
            return new TxInput
            (
                previousTxOutput ?? this.PrevTxOutputKey,
                scriptSignature ?? this.ScriptSignature,
                sequence ?? this.Sequence
            );
        }
    }
}
