using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Core.Builders
{
    internal class UnconfirmedTxesBuilder : IDisposable
    {
        private readonly ICoreDaemon coreDaemon;
        private readonly IStorageManager storageManager;

        private readonly SemaphoreSlim addBlockLock = new SemaphoreSlim(1);
        private readonly ReaderWriterLockSlim commitLock = new ReaderWriterLockSlim();

        private bool disposed;

        public UnconfirmedTxesBuilder(ICoreDaemon coreDaemon, IStorageManager storageManager)
        {
            this.coreDaemon = coreDaemon;
            this.storageManager = storageManager;
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
                addBlockLock.Dispose();
                commitLock.Dispose();

                disposed = true;
            }
        }

        public bool ContainsTransaction(UInt256 txHash)
        {
            using (var handle = storageManager.OpenUnconfirmedTxesCursor())
            {
                var unconfirmedTxesCursor = handle.Item;
                unconfirmedTxesCursor.BeginTransaction(readOnly: true);

                return unconfirmedTxesCursor.ContainsTransaction(txHash);
            }
        }

        public bool TryAddTransaction(Transaction tx)
        {
            if (ContainsTransaction(tx.Hash))
                // unconfirmed tx already exists
                return false;

            // take addBlockLock, cannot add unconfirmed txes while they are being updated by a block operation
            return addBlockLock.Do(() =>
            {
                using (var chainState = coreDaemon.GetChainState())
                {
                    // verify each input is available to spend
                    var prevTxOutputKeys = new HashSet<TxOutputKey>();
                    for (var inputIndex = 0; inputIndex < tx.Inputs.Length; inputIndex++)
                    {
                        var input = tx.Inputs[inputIndex];

                        if (!prevTxOutputKeys.Add(input.PreviousTxOutputKey))
                            // tx double spends one of its own inputs
                            return false;

                        UnspentTx unspentTx;
                        if (!chainState.TryGetUnspentTx(input.PrevTxHash, out unspentTx))
                            // input's prev output does not exist
                            return false;

                        if (input.PrevTxOutputIndex >= unspentTx.OutputStates.Length)
                            // input's prev output does not exist
                            return false;

                        if (unspentTx.OutputStates[(int)input.PrevTxOutputIndex] != OutputState.Unspent)
                            // input's prev output has already been spent
                            return false;
                    }

                    // validation passed

                    // create the unconfirmed tx
                    var unconfirmedTx = new UnconfirmedTx(tx);

                    // add the unconfirmed tx
                    using (var handle = storageManager.OpenUnconfirmedTxesCursor())
                    {
                        var unconfirmedTxesCursor = handle.Item;

                        unconfirmedTxesCursor.BeginTransaction();
                        if (unconfirmedTxesCursor.TryAddTransaction(unconfirmedTx))
                        {
                            unconfirmedTxesCursor.CommitTransaction();
                            return true;
                        }
                        else
                            // unconfirmed tx already exists
                            return false;
                    }
                }
            });

            throw new NotImplementedException();
        }

        public bool TryGetTransaction(UInt256 txHash, out UnconfirmedTx unconfirmedTx)
        {
            using (var handle = storageManager.OpenUnconfirmedTxesCursor())
            {
                var unconfirmedTxesCursor = handle.Item;
                unconfirmedTxesCursor.BeginTransaction(readOnly: true);

                return unconfirmedTxesCursor.TryGetTransaction(txHash, out unconfirmedTx);
            }
        }

        public ImmutableList<Transaction> GetTransactionsSpending(UInt256 txHash, int outputIndex)
        {
            throw new NotImplementedException();
        }


        public async Task AddBlockAsync(ChainedHeader chainedHeader, IEnumerable<BlockTx> blockTxes, CancellationToken cancelToken = default(CancellationToken))
        {
            await addBlockLock.DoAsync(async () =>
            {

            });
        }

        public void RollbackBlock(ChainedHeader chainedHeader, IEnumerable<BlockTx> blockTxes)
        {
            addBlockLock.Do(() =>
            {

            });
        }

        //public UnconfirmedTxPool ToImmutable()
        //{
        //    return commitLock.DoRead(() =>
        //        new UnconfirmedTxPool(chain.Value, storageManager));
        //}
    }
}
