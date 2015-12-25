using BitSharp.Common;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Core.Domain
{
    public class BlockTx
    {
        public BlockTx(int index, UInt256 hash, ImmutableArray<byte> txBytes)
        {
            Hash = hash;
            Index = index;
            EncodedTx = new EncodedTx(hash, txBytes);
        }

        public BlockTx(int index, EncodedTx encodedTx)
        {
            if (encodedTx == null)
                throw new ArgumentNullException(nameof(encodedTx));

            Hash = encodedTx.Hash;
            Index = index;
            EncodedTx = encodedTx;
        }

        public UInt256 Hash { get; }

        public int Index { get; }

        public bool IsCoinbase => this.Index == 0;

        public EncodedTx EncodedTx { get; }

        public DecodedBlockTx Decode()
        {
            return new DecodedBlockTx(Index, EncodedTx.Decode());
        }

        public static DecodedBlockTx Create(int txIndex, Transaction tx)
        {
            if (tx == null)
                throw new ArgumentNullException(nameof(tx));

            var txBytes = DataEncoder.EncodeTransaction(tx).ToImmutableArray();
            var decodedTx = new DecodedTx(txBytes, tx);

            return new DecodedBlockTx(txIndex, decodedTx);
        }

        public static implicit operator BlockTxNode(BlockTx tx)
        {
            return new BlockTxNode(tx.Index, 0, tx.Hash, false, tx.EncodedTx);
        }
    }
}
