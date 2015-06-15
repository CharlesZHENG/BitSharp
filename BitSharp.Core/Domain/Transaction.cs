using BitSharp.Common;
using System;
using System.Collections.Immutable;

namespace BitSharp.Core.Domain
{
    public class Transaction
    {
        private readonly UInt32 _version;
        private readonly ImmutableArray<TxInput> _inputs;
        private readonly ImmutableArray<TxOutput> _outputs;
        private readonly UInt32 _lockTime;
        private readonly UInt256 _hash;

        public Transaction(UInt32 version, ImmutableArray<TxInput> inputs, ImmutableArray<TxOutput> outputs, UInt32 lockTime, UInt256 hash = null)
        {
            this._version = version;
            this._inputs = inputs;
            this._outputs = outputs;
            this._lockTime = lockTime;

            this._hash = hash ?? DataCalculator.CalculateTransactionHash(version, inputs, outputs, lockTime);
        }

        public UInt32 Version { get { return this._version; } }

        public ImmutableArray<TxInput> Inputs { get { return this._inputs; } }

        public ImmutableArray<TxOutput> Outputs { get { return this._outputs; } }

        public UInt32 LockTime { get { return this._lockTime; } }

        public UInt256 Hash { get { return this._hash; } }

        public Transaction With(UInt32? Version = null, ImmutableArray<TxInput>? Inputs = null, ImmutableArray<TxOutput>? Outputs = null, UInt32? LockTime = null)
        {
            return new Transaction
            (
                Version ?? this.Version,
                Inputs ?? this.Inputs,
                Outputs ?? this.Outputs,
                LockTime ?? this.LockTime
            );
        }
    }
}
