using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Core.Domain
{
    public class ValidatableTx
    {
        public ValidatableTx(BlockTx blockTx, ChainedHeader chainedHeader, ImmutableArray<TxOutput> prevTxOutputs)
        {
            BlockTx = blockTx;
            ChainedHeader = chainedHeader;
            PrevTxOutputs = prevTxOutputs;
        }

        public BlockTx BlockTx { get; }

        public ChainedHeader ChainedHeader { get; }

        public ImmutableArray<TxOutput> PrevTxOutputs { get; }
    }
}
