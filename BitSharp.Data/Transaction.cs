﻿using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Data.ExtensionMethods;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;

namespace BitSharp.Data
{
    public class Transaction
    {
        private readonly UInt32 _version;
        private readonly ImmutableArray<TxInput> _inputs;
        private readonly ImmutableArray<TxOutput> _outputs;
        private readonly UInt32 _lockTime;
        private readonly UInt256 _hash;
        private readonly long _sizeEstimate;

        private readonly int hashCode;

        public Transaction(UInt32 version, ImmutableArray<TxInput> inputs, ImmutableArray<TxOutput> outputs, UInt32 lockTime, UInt256 hash = null)
        {
            this._version = version;
            this._inputs = inputs;
            this._outputs = outputs;
            this._lockTime = lockTime;

            var sizeEstimate = 0L;
            for (var i = 0; i < inputs.Length; i++)
                sizeEstimate += inputs[i].ScriptSignature.Length;

            for (var i = 0; i < outputs.Length; i++)
                sizeEstimate += outputs[i].ScriptPublicKey.Length;
            sizeEstimate = (long)(sizeEstimate * 1.5);
            this._sizeEstimate = sizeEstimate;

            this._hash = hash ?? DataCalculator.CalculateTransactionHash(version, inputs, outputs, lockTime);

            this.hashCode = this._hash.GetHashCode();
        }

        public UInt32 Version { get { return this._version; } }

        public ImmutableArray<TxInput> Inputs { get { return this._inputs; } }

        public ImmutableArray<TxOutput> Outputs { get { return this._outputs; } }

        public UInt32 LockTime { get { return this._lockTime; } }

        public UInt256 Hash { get { return this._hash; } }

        public long SizeEstimate { get { return this._sizeEstimate; } }

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

        public override bool Equals(object obj)
        {
            if (!(obj is Transaction))
                return false;

            return (Transaction)obj == this;
        }

        public override int GetHashCode()
        {
            return this.hashCode;
        }

        public static bool operator ==(Transaction left, Transaction right)
        {
            return object.ReferenceEquals(left, right) || (!object.ReferenceEquals(left, null) && !object.ReferenceEquals(right, null) && left.Hash == right.Hash && left.Version == right.Version && left.Inputs.SequenceEqual(right.Inputs) && left.Outputs.SequenceEqual(right.Outputs) && left.LockTime == right.LockTime);
        }

        public static bool operator !=(Transaction left, Transaction right)
        {
            return !(left == right);
        }

        public static long SizeEstimator(Transaction tx)
        {
            return tx.SizeEstimate;
        }
    }
}
