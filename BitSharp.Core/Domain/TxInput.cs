using BitSharp.Common;
using System;
using System.Collections.Immutable;

namespace BitSharp.Core.Domain
{
    public class TxInput
    {
        public TxInput(TxOutputKey previousTxOutputKey, ImmutableArray<byte> scriptSignature, UInt32 sequence)
        {
            PreviousTxOutputKey = previousTxOutputKey;
            ScriptSignature = scriptSignature;
            Sequence = sequence;
        }

        public TxInput(UInt256 prevTxHash, uint prevTxOutputIndex, ImmutableArray<byte> scriptSignature, UInt32 sequence)
            : this(new TxOutputKey(prevTxHash, prevTxOutputIndex), scriptSignature, sequence)
        { }

        public TxOutputKey PreviousTxOutputKey { get; }

        public ImmutableArray<byte> ScriptSignature { get; }

        public UInt32 Sequence { get; }

        public TxInput With(TxOutputKey previousTxOutput = null, ImmutableArray<byte>? scriptSignature = null, UInt32? sequence = null)
        {
            return new TxInput
            (
                previousTxOutput ?? this.PreviousTxOutputKey,
                scriptSignature ?? this.ScriptSignature,
                sequence ?? this.Sequence
            );
        }
    }
}
