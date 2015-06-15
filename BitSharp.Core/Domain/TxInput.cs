using System;

namespace BitSharp.Core.Domain
{
    public class TxInput
    {
        private readonly TxOutputKey _previousTxOutputKey;
        private readonly byte[] _scriptSignature;
        private readonly UInt32 _sequence;

        public TxInput(TxOutputKey previousTxOutputKey, byte[] scriptSignature, UInt32 sequence)
        {
            this._previousTxOutputKey = previousTxOutputKey;
            this._scriptSignature = scriptSignature;
            this._sequence = sequence;
        }

        public TxOutputKey PreviousTxOutputKey { get { return this._previousTxOutputKey; } }

        public byte[] ScriptSignature { get { return this._scriptSignature; } }

        public UInt32 Sequence { get { return this._sequence; } }

        public TxInput With(TxOutputKey previousTxOutput = null, byte[] scriptSignature = null, UInt32? sequence = null)
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
