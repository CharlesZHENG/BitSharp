using BitSharp.Common;
using BitSharp.Core.Domain;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Core.Builders
{
    internal class UnconfirmedTxesBuilder
    {
        private bool disposed;

        public UnconfirmedTxesBuilder()
        {
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposed && disposing)
            {
                disposed = true;
            }
        }

        public void AddTransaction(Transaction tx)
        {

        }

        public ImmutableList<Transaction> GetTransactionsSpending(UInt256 txHash, int outputIndex)
        {
            throw new NotImplementedException();
        }


        public async Task AddBlockAsync(ChainedHeader chainedHeader, IEnumerable<BlockTx> blockTxes, CancellationToken cancelToken = default(CancellationToken))
        {
            throw new NotImplementedException();
        }

        public void RollbackBlock(ChainedHeader chainedHeader, IEnumerable<BlockTx> blockTxes)
        {
            throw new NotImplementedException();
        }

        //public UnconfirmedTxPool ToImmutable()
        //{
        //    return commitLock.DoRead(() =>
        //        new UnconfirmedTxPool(chain.Value, storageManager));
        //}
    }
}
