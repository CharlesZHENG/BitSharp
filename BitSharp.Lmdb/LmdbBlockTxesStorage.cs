using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage;
using LightningDB;
using NLog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace BitSharp.Lmdb
{
    public class LmdbBlockTxesStorage : IBlockTxesStorage
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly string jetDirectory;
        private readonly LightningEnvironment jetInstance;
        private readonly LightningDatabase globalsTableId;
        private readonly LightningDatabase blocksTableId;

        private readonly byte[] blockCountKey = UTF8Encoding.ASCII.GetBytes("BlockCount");

        private bool isDisposed;

        public LmdbBlockTxesStorage(string baseDirectory, long blockTxesSize, int? index = null)
        {
            this.jetDirectory = Path.Combine(baseDirectory, "BlockTxes");
            if (index.HasValue)
                this.jetDirectory = Path.Combine(jetDirectory, index.Value.ToString());

            LmdbStorageManager.PrepareSparseDatabase(this.jetDirectory);
            this.jetInstance = new LightningEnvironment(this.jetDirectory, EnvironmentOpenFlags.NoThreadLocalStorage | EnvironmentOpenFlags.NoSync)
            {
                MaxDatabases = 10,
                MapSize = blockTxesSize
            };
            this.jetInstance.Open();

            using (var txn = this.jetInstance.BeginTransaction())
            {
                globalsTableId = txn.OpenDatabase("Globals", new DatabaseOptions { Flags = DatabaseOpenFlags.Create });
                blocksTableId = txn.OpenDatabase("Blocks", new DatabaseOptions { Flags = DatabaseOpenFlags.Create });

                if (!txn.ContainsKey(globalsTableId, blockCountKey))
                    txn.Put(globalsTableId, blockCountKey, Bits.GetBytes(0));

                txn.Commit();
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!isDisposed && disposing)
            {
                this.jetInstance.Dispose();

                isDisposed = true;
            }
        }

        public bool ContainsBlock(UInt256 blockHash)
        {
            using (var txn = this.jetInstance.BeginTransaction(TransactionBeginFlags.ReadOnly))
            {
                return txn.ContainsKey(blocksTableId, DbEncoder.EncodeBlockHashTxIndex(blockHash, 0));
            }
        }

        public void PruneElements(IEnumerable<KeyValuePair<UInt256, IEnumerable<int>>> blockTxIndices)
        {
            foreach (var keyPair in blockTxIndices)
            {
                var blockHash = keyPair.Key;
                var txIndices = keyPair.Value;

                using (var txn = this.jetInstance.BeginTransaction())
                using (var cursor = txn.CreateCursor(blocksTableId))
                {
                    var pruningCursor = new MerkleTreePruningCursor(blockHash, txn, blocksTableId, cursor);
                    var cachedCursor = new CachedMerkleTreePruningCursor<BlockTxNode>(pruningCursor);

                    // prune the transactions
                    foreach (var index in txIndices)
                        MerkleTree.PruneNode(cachedCursor, index);

                    cursor.Dispose();
                    txn.Commit();
                }
            }
        }

        public void DeleteElements(IEnumerable<KeyValuePair<UInt256, IEnumerable<int>>> blockTxIndices)
        {
            foreach (var keyPair in blockTxIndices)
            {
                var blockHash = keyPair.Key;
                var txIndices = keyPair.Value;
                using (var txn = this.jetInstance.BeginTransaction())
                {

                    // prune the transactions
                    foreach (var index in txIndices)
                    {
                        var key = DbEncoder.EncodeBlockHashTxIndex(blockHash, index);
                        if (txn.ContainsKey(blocksTableId, key))
                            txn.Delete(blocksTableId, key);
                    }

                    txn.Commit();
                }
            }
        }

        public bool TryReadBlockTransactions(UInt256 blockHash, out IEnumerator<BlockTx> blockTxes)
        {
            if (this.ContainsBlock(blockHash))
            {
                blockTxes = ReadBlockTransactions(blockHash, requireTx: true)
                    .UsingAsEnumerable().Select(x => x.ToBlockTx()).GetEnumerator();
                return true;
            }
            else
            {
                blockTxes = null;
                return false;
            }
        }

        public bool TryReadBlockTxNodes(UInt256 blockHash, out IEnumerator<BlockTxNode> blockTxNodes)
        {
            if (this.ContainsBlock(blockHash))
            {
                blockTxNodes = ReadBlockTransactions(blockHash, requireTx: false);
                return true;
            }
            else
            {
                blockTxNodes = null;
                return false;
            }
        }

        private IEnumerator<BlockTxNode> ReadBlockTransactions(UInt256 blockHash, bool requireTx)
        {
            using (var txn = this.jetInstance.BeginTransaction(TransactionBeginFlags.ReadOnly))
            using (var cursor = txn.CreateCursor(blocksTableId))
            {
                var kvPair = cursor.MoveToFirstAfter(DbEncoder.EncodeBlockHashTxIndex(blockHash, 0));

                if (kvPair == null)
                    yield break;

                do
                {
                    UInt256 recordBlockHash; int txIndex;
                    DbEncoder.DecodeBlockHashTxIndex(kvPair.Value.Key, out recordBlockHash, out txIndex);
                    if (blockHash != recordBlockHash)
                        yield break;

                    var blockTx = DataEncoder.DecodeBlockTxNode(kvPair.Value.Value);

                    if (requireTx && blockTx.Pruned)
                        throw new MissingDataException(blockHash);

                    yield return blockTx;
                }
                while ((kvPair = cursor.MoveNext()) != null);
            }
        }

        public bool TryGetTransaction(UInt256 blockHash, int txIndex, out BlockTx transaction)
        {
            using (var txn = this.jetInstance.BeginTransaction(TransactionBeginFlags.ReadOnly))
            {
                byte[] blockTxBytes;
                if (txn.TryGet(blocksTableId, DbEncoder.EncodeBlockHashTxIndex(blockHash, txIndex), out blockTxBytes))
                {
                    var blockTxNode = DataEncoder.DecodeBlockTxNode(blockTxBytes);
                    if (!blockTxNode.Pruned)
                    {
                        transaction = blockTxNode.ToBlockTx();
                        return true;
                    }
                    else
                    {
                        transaction = null;
                        return false;
                    }
                }
                else
                {
                    transaction = null;
                    return false;
                }
            }
        }

        public int BlockCount
        {
            get
            {
                using (var txn = this.jetInstance.BeginTransaction(TransactionBeginFlags.ReadOnly))
                {
                    return Bits.ToInt32(txn.Get(globalsTableId, blockCountKey));
                }
            }
        }

        public string Name => "Blocks";

        public bool TryAddBlockTransactions(UInt256 blockHash, IEnumerable<EncodedTx> blockTxes)
        {
            try
            {
                if (this.ContainsBlock(blockHash))
                    return false;

                using (var txn = this.jetInstance.BeginTransaction())
                {
                    var txIndex = 0;
                    foreach (var tx in blockTxes)
                    {
                        var blockTx = new BlockTx(txIndex, tx);

                        var key = DbEncoder.EncodeBlockHashTxIndex(blockHash, txIndex);
                        var value = DataEncoder.EncodeBlockTxNode(blockTx);

                        txn.Put(blocksTableId, key, value);
                        txIndex++;
                    }

                    // increase block count
                    txn.Put(globalsTableId, blockCountKey,
                        Bits.ToInt32(txn.Get(globalsTableId, blockCountKey)) + 1);

                    txn.Commit();
                    return true;
                }
            }
            catch (Exception)
            {
                return false;
            }
        }

        public bool TryRemoveBlockTransactions(UInt256 blockHash)
        {
            using (var txn = this.jetInstance.BeginTransaction())
            using (var cursor = txn.CreateCursor(blocksTableId))
            {
                // remove transactions
                var kvPair = cursor.MoveToFirstAfter(DbEncoder.EncodeBlockHashTxIndex(blockHash, 0));

                var removed = false;
                if (kvPair != null)
                {
                    do
                    {
                        UInt256 recordBlockHash; int txIndex;
                        DbEncoder.DecodeBlockHashTxIndex(kvPair.Value.Key, out recordBlockHash, out txIndex);
                        if (blockHash == recordBlockHash)
                        {
                            cursor.Delete();
                            removed = true;
                        }
                        else
                        {
                            break;
                        }
                    }
                    while ((kvPair = cursor.MoveNext()) != null);
                }

                if (removed)
                {
                    // decrease block count
                    txn.Put(globalsTableId, blockCountKey,
                        Bits.ToInt32(txn.Get(globalsTableId, blockCountKey)) - 1);

                    cursor.Dispose();
                    txn.Commit();
                }

                return removed;
            }
        }

        public void Flush()
        {
            this.jetInstance.Flush(force: true);
        }

        public void Defragment()
        {
            logger.Info($"BlockTxes database: {this.jetInstance.UsedSize / 1.MILLION():N0} MB");
        }
    }
}
