using BitSharp.Common;
using System;
using System.Collections.Immutable;

namespace BitSharp.Core.Domain
{
    public class Transaction
    {
        public Transaction(UInt32 version, ImmutableArray<TxInput> inputs, ImmutableArray<TxOutput> outputs, UInt32 lockTime, UInt256 hash = null)
        {
            Version = version;
            Inputs = inputs;
            Outputs = outputs;
            LockTime = lockTime;

            this.Hash = hash ?? DataCalculator.CalculateTransactionHash(version, inputs, outputs, lockTime);
        }

        public UInt32 Version { get; }

        public ImmutableArray<TxInput> Inputs { get; }

        public ImmutableArray<TxOutput> Outputs { get; }

        public UInt32 LockTime { get; }

        public UInt256 Hash { get; }

        public Transaction With(UInt32? Version = null, ImmutableArray<TxInput>? Inputs = null, ImmutableArray<TxOutput>? Outputs = null, UInt32? LockTime = null, UInt256 Hash = null)
        {
            return new Transaction
            (
                Version ?? this.Version,
                Inputs ?? this.Inputs,
                Outputs ?? this.Outputs,
                LockTime ?? this.LockTime,
                Hash
            );
        }
    }
}
