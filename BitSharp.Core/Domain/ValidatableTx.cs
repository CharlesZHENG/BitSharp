using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Core.Domain
{
    public class ValidatableTx
    {
        public ValidatableTx(DecodedBlockTx blockTx, ChainedHeader chainedHeader, ImmutableArray<PrevTxOutput> prevTxOutputs)
        {
            if (blockTx == null)
                throw new ArgumentNullException(nameof(blockTx));
            if (chainedHeader == null)
                throw new ArgumentNullException(nameof(chainedHeader));

            BlockTx = blockTx;
            ChainedHeader = chainedHeader;
            PrevTxOutputs = prevTxOutputs;
        }

        public DecodedBlockTx BlockTx { get; }

        public ChainedHeader ChainedHeader { get; }

        public ImmutableArray<PrevTxOutput> PrevTxOutputs { get; }

        public Transaction Transaction => BlockTx.Transaction;

        public UInt256 Hash => BlockTx.Hash;

        public int Index => BlockTx.Index;

        public bool IsCoinbase => BlockTx.IsCoinbase;

        public ImmutableArray<byte> TxBytes => BlockTx.TxBytes;
    }
}
