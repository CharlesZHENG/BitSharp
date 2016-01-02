using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage;
using NLog;
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
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly ICoreDaemon coreDaemon;
        private readonly IStorageManager storageManager;

        private readonly ReaderWriterLockSlim updateLock = new ReaderWriterLockSlim();
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
                updateLock.Dispose();
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

            // allow concurrent transaction adds if underlying storage supports it
            // in either case, lock waits for block add/rollback to finish
            if (storageManager.IsUnconfirmedTxesConcurrent)
                updateLock.EnterReadLock();
            else
                updateLock.EnterWriteLock();
            try
            {
                using (var chainState = coreDaemon.GetChainState())
                {
                    // verify each input is available to spend
                    var prevTxOutputKeys = new HashSet<TxOutputKey>();
                    for (var inputIndex = 0; inputIndex < tx.Inputs.Length; inputIndex++)
                    {
                        var input = tx.Inputs[inputIndex];

                        if (!prevTxOutputKeys.Add(input.PrevTxOutputKey))
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
            }
            finally
            {
                if (storageManager.IsUnconfirmedTxesConcurrent)
                    updateLock.ExitReadLock();
                else
                    updateLock.ExitWriteLock();
            }

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

        public ImmutableDictionary<UInt256, UnconfirmedTx> GetTransactionsSpending(UInt256 txHash, uint outputIndex)
        {
            return GetTransactionsSpending(new TxOutputKey(txHash, outputIndex));
        }

        public ImmutableDictionary<UInt256, UnconfirmedTx> GetTransactionsSpending(TxOutputKey txOutputKey)
        {
            using (var handle = storageManager.OpenUnconfirmedTxesCursor())
            {
                var unconfirmedTxesCursor = handle.Item;
                unconfirmedTxesCursor.BeginTransaction(readOnly: true);

                return unconfirmedTxesCursor.GetTransactionsSpending(txOutputKey);
            }
        }

        public void AddBlock(ChainedHeader chainedHeader, IEnumerable<BlockTx> blockTxes, CancellationToken cancelToken = default(CancellationToken))
        {
            updateLock.EnterWriteLock();
            try
            {
                using (var handle = storageManager.OpenUnconfirmedTxesCursor())
                {
                    var unconfirmedTxesCursor = handle.Item;

                    unconfirmedTxesCursor.BeginTransaction();

                    foreach (var blockTx in blockTxes)
                    {
                        // remove any txes confirmed in the block from the list of unconfirmed txes
                        if (!unconfirmedTxesCursor.TryRemoveTransaction(blockTx.Hash))
                        {
                            var confirmedTx = blockTx.EncodedTx.Decode().Transaction;

                            // check for and remove any unconfirmed txes that conflict with the confirmed tx
                            foreach (var input in confirmedTx.Inputs)
                            {
                                var conflictingTxes = unconfirmedTxesCursor.GetTransactionsSpending(input.PrevTxOutputKey);
                                if (conflictingTxes.Count > 0)
                                {
                                    logger.Warn($"Removing {conflictingTxes.Count} conflicting txes from the unconfirmed transaction pool");

                                    // remove the conflicting unconfirmed txes
                                    foreach (var conflictingTx in conflictingTxes.Keys)
                                        if (!unconfirmedTxesCursor.TryRemoveTransaction(conflictingTx))
                                            throw new StorageCorruptException(StorageType.UnconfirmedTxes, $"{conflictingTx} is indexed but not present");
                                }
                            }
                        }
                    }

                    unconfirmedTxesCursor.CommitTransaction();
                }
            }
            finally
            {
                updateLock.ExitWriteLock();
            }
        }

        public void RollbackBlock(ChainedHeader chainedHeader, IEnumerable<BlockTx> blockTxes)
        {
            updateLock.DoWrite(() =>
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
