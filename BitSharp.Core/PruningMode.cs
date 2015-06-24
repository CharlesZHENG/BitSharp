using System;

namespace BitSharp.Core
{
    [Flags]
    public enum PruningMode
    {
        None = 0,

        // remove the TxHash->Block+TxIndex mapping for fully spent transactions
        // this information is needed to replay blocks, it is used to load the transactions referenced by inputs
        TxIndex = 1,

        // remove the Block->List<TxHash> mapping of which transactions were fully spent within a block
        // this information is needed to prune the tx index and block txes storage, it indicates which transactions can be pruned
        BlockSpentIndex = 2,

        // remove fully spent transactions from block storage, preserving the merkle tree
        BlockTxesPreserveMerkle = 4,

        // remove fully spent transactions from block storage, without preserving the merkle tree
        BlockTxesDestroyMerkle = 8
    }
}
