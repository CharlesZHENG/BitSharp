using BitSharp.Common;
using System;
using System.Collections.Immutable;
using System.Linq;
using System.IO;

namespace BitSharp.Core.Domain
{
    public class EncodedTx
    {
        private readonly Lazy<Transaction> lazyTx;

        public EncodedTx(UInt256 hash, ImmutableArray<byte> txBytes)
        {
            Hash = hash;
            TxBytes = txBytes;
            lazyTx = new Lazy<Transaction>(() => DataDecoder.DecodeTransaction(hash, txBytes.ToArray()));
        }

        public EncodedTx(ImmutableArray<byte> txBytes, Transaction transaction)
        {
            Hash = transaction.Hash;
            TxBytes = txBytes;
            lazyTx = new Lazy<Transaction>(() => transaction).Force();
        }

        public UInt256 Hash { get; }

        public ImmutableArray<byte> TxBytes { get; }

        public DecodedTx Decode() => new DecodedTx(TxBytes, lazyTx.Value);
    }
}
