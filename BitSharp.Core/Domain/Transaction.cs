using BitSharp.Common;
using System;
using System.Collections.Immutable;

namespace BitSharp.Core.Domain
{
    public class Transaction
    {
        public Transaction(UInt32 version, ImmutableArray<TxInput> inputs, ImmutableArray<TxOutput> outputs, UInt32 lockTime, UInt256 hash)
        {
            if (hash == null)
                throw new ArgumentNullException(nameof(hash));

            Hash = hash;
            IsCoinbase =
                inputs.Length == 1
                && inputs[0].PrevTxOutputKey.TxHash == UInt256.Zero
                && inputs[0].PrevTxOutputKey.TxOutputIndex == uint.MaxValue;
            Version = version;
            Inputs = inputs;
            Outputs = outputs;
            LockTime = lockTime;
        }

        public UInt256 Hash { get; }

        public bool IsCoinbase { get; }

        public UInt32 Version { get; }

        public ImmutableArray<TxInput> Inputs { get; }

        public ImmutableArray<TxOutput> Outputs { get; }

        public UInt32 LockTime { get; }

        public DecodedTx CreateWith(UInt32? Version = null, ImmutableArray<TxInput>? Inputs = null, ImmutableArray<TxOutput>? Outputs = null, UInt32? LockTime = null, UInt256 Hash = null)
        {
            return Create
            (
                Version ?? this.Version,
                Inputs ?? this.Inputs,
                Outputs ?? this.Outputs,
                LockTime ?? this.LockTime
            );
        }

        public static DecodedTx Create(UInt32 version, ImmutableArray<TxInput> inputs, ImmutableArray<TxOutput> outputs, UInt32 lockTime)
        {
            return DataEncoder.EncodeTransaction(version, inputs, outputs, lockTime);
        }
    }
}
