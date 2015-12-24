using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Core.Domain
{
    public class ValidatableTx
    {
        public ValidatableTx(BlockTx transaction, ChainedHeader chainedHeader, ImmutableArray<TxOutput> prevTxOutputs)
        {
            Transaction = transaction;
            ChainedHeader = chainedHeader;
            PrevTxOutputs = prevTxOutputs;
        }

        public BlockTx Transaction { get; }

        public ChainedHeader ChainedHeader { get; }

        public ImmutableArray<TxOutput> PrevTxOutputs { get; }
    }
}
