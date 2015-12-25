using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using System;

namespace BitSharp.Core.Test
{
    public static class ExtensionMethods
    {
        public static Block WithAddedTransactions(this Block block, params Transaction[] transactions)
        {
            // update transactions
            block = block.CreateWith(Transactions: block.Transactions.AddRange(transactions));

            // update merkle root
            block = block.CreateWith(block.Header.With(MerkleRoot: MerkleTree.CalculateMerkleRoot(block.Transactions)));

            return block;
        }

        public static UInt64 OutputValue(this Transaction transaction)
        {
            return transaction.Outputs.Sum(x => x.Value);
        }
    }
}
