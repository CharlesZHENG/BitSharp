using BitSharp.Common;
using BitSharp.Core.Domain;
using System;
using System.Collections.Generic;

namespace BitSharp.Core.Storage
{
    public interface ICoreStorage : IDisposable
    {
        bool TryGetChainedHeader(UInt256 blockHash, out ChainedHeader chainedHeader);

        bool TryRemoveChainedHeader(UInt256 blockHash);

        bool TryReadChain(UInt256 blockHash, out Chain chain);

        bool TryReadBlockTransactions(UInt256 blockHash, out IEnumerator<BlockTx> blockTxes);

        bool TryRemoveBlockTransactions(UInt256 blockHash);

        bool TryGetTransaction(UInt256 blockHash, int txIndex, out BlockTx transaction);
    }
}
