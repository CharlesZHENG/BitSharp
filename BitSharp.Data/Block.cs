﻿using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace BitSharp.Data
{
    public class Block
    {
        private readonly BlockHeader _header;
        private readonly ImmutableArray<Transaction> _transactions;
        private readonly long _sizeEstimate;

        public Block(BlockHeader header, ImmutableArray<Transaction> transactions)
        {
            this._header = header;
            this._transactions = transactions;

            var sizeEstimate = BlockHeader.SizeEstimator(header);
            for (var i = 0; i < transactions.Length; i++)
            {
                for (var j = 0; j < transactions[i].Inputs.Length; j++)
                    sizeEstimate += transactions[i].Inputs[j].ScriptSignature.Length;

                for (var j = 0; j < transactions[i].Outputs.Length; j++)
                    sizeEstimate += transactions[i].Outputs[j].ScriptPublicKey.Length;
            }
            sizeEstimate = (long)(sizeEstimate * 1.5);

            this._sizeEstimate = sizeEstimate;
        }

        public UInt256 Hash { get { return this.Header.Hash; } }

        public BlockHeader Header { get { return this._header; } }

        public ImmutableArray<Transaction> Transactions { get { return this._transactions; } }

        public long SizeEstimate { get { return this._sizeEstimate; } }

        public Block With(BlockHeader Header = null, ImmutableArray<Transaction>? Transactions = null)
        {
            return new Block
            (
                Header ?? this.Header,
                Transactions ?? this.Transactions
            );
        }

        //TODO for unit test, move elsewhere
        public Block WithAddedTransactions(params Transaction[] transactions)
        {
            return this.With(Transactions: this.Transactions.AddRange(transactions));
        }

        public override bool Equals(object obj)
        {
            if (!(obj is Block))
                return false;

            return (Block)obj == this;
        }

        public static bool operator ==(Block left, Block right)
        {
            return object.ReferenceEquals(left, right) || (!object.ReferenceEquals(left, null) && !object.ReferenceEquals(right, null) && left.Header == right.Header && left.Transactions.SequenceEqual(right.Transactions));
        }

        public static bool operator !=(Block left, Block right)
        {
            return !(left == right);
        }

        public static long SizeEstimator(Block block)
        {
            return block.SizeEstimate;
        }
    }
}
