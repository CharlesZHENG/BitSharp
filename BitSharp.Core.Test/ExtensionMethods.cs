using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using System;
using System.Linq;

namespace BitSharp.Core.Test
{
    public static class ExtensionMethods
    {
        public static Block CreateWithAddedTransactions(this Block block, params EncodedTx[] transactions)
        {
            // update transactions
            var updatedTxes = block.BlockTxes.AddRange(transactions.Select((tx, txIndex) =>
                new BlockTx(block.BlockTxes.Length + txIndex, tx)));

            // update merkle root
            var updatedHeader = block.Header.With(MerkleRoot: MerkleTree.CalculateMerkleRoot(updatedTxes));

            return new Block(updatedHeader, updatedTxes);
        }

        public static UInt64 OutputValue(this Transaction transaction)
        {
            return transaction.Outputs.Sum(x => x.Value);
        }
    }
}
