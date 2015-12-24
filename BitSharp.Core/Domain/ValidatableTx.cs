using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Core.Domain
{
    public class ValidatableTx
    {
        public ValidatableTx(int txIndex, Transaction transaction, ChainedHeader chainedHeader, ImmutableArray<TxOutput> prevTxOutputs)
        {
            Transaction = transaction;
            TxIndex = txIndex;
            ChainedHeader = chainedHeader;
            PrevTxOutputs = prevTxOutputs;
        }

        public bool IsCoinbase => this.TxIndex == 0;

        public Transaction Transaction { get; }

        public int TxIndex { get; }

        public ChainedHeader ChainedHeader { get; }

        public ImmutableArray<TxOutput> PrevTxOutputs { get; }
    }
}
