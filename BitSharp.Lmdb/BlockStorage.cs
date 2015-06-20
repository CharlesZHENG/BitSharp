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

namespace BitSharp.Lmdb
{
    public class BlockStorage : IBlockStorage
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly string jetDirectory;
        public readonly LightningEnvironment jetInstance;
        public readonly LightningDatabase globalsTableId;
        public readonly LightningDatabase blockHeadersTableId;
        public readonly LightningDatabase invalidBlocksTableId;

        private bool isDisposed;

        public BlockStorage(string baseDirectory, long blocksSize)
        {
            this.jetDirectory = Path.Combine(baseDirectory, "Blocks");

            LmdbStorageManager.PrepareSparseDatabase(this.jetDirectory);
            this.jetInstance = new LightningEnvironment(this.jetDirectory, EnvironmentOpenFlags.NoThreadLocalStorage | EnvironmentOpenFlags.NoSync)
            {
                MaxDatabases = 10,
                MapSize = blocksSize,
            };
            this.jetInstance.Open();

            using (var txn = this.jetInstance.BeginTransaction())
            {
                globalsTableId = txn.OpenDatabase("Globals", new DatabaseOptions { Flags = DatabaseOpenFlags.Create });
                blockHeadersTableId = txn.OpenDatabase("BlockHeaders", new DatabaseOptions { Flags = DatabaseOpenFlags.Create });
                invalidBlocksTableId = txn.OpenDatabase("InvalidBlocks", new DatabaseOptions { Flags = DatabaseOpenFlags.Create });

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

        public bool ContainsChainedHeader(UInt256 blockHash)
        {
            using (var txn = this.jetInstance.BeginTransaction(TransactionBeginFlags.ReadOnly))
            {
                return txn.ContainsKey(blockHeadersTableId, DbEncoder.EncodeUInt256(blockHash));
            }
        }

        public bool TryAddChainedHeader(ChainedHeader chainedHeader)
        {
            try
            {
                using (var txn = this.jetInstance.BeginTransaction())
                {
                    var key = DbEncoder.EncodeUInt256(chainedHeader.Hash);

                    if (!txn.ContainsKey(blockHeadersTableId, key))
                    {
                        var value = DataEncoder.EncodeChainedHeader(chainedHeader);

                        txn.Put(blockHeadersTableId, key, value);

                        txn.Commit();

                        if (cachedMaxHeader != null && chainedHeader.TotalWork > cachedMaxHeader.TotalWork)
                            cachedMaxHeader = chainedHeader;

                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
            }
            catch (Exception)
            {
                return false;
            }
        }

        public bool TryGetChainedHeader(UInt256 blockHash, out ChainedHeader chainedHeader)
        {
            using (var txn = this.jetInstance.BeginTransaction(TransactionBeginFlags.ReadOnly))
            {
                byte[] chainedHeaderBytes;
                if (txn.TryGet(blockHeadersTableId, DbEncoder.EncodeUInt256(blockHash), out chainedHeaderBytes))
                {
                    chainedHeader = DataEncoder.DecodeChainedHeader(chainedHeaderBytes);
                    return true;
                }
                else
                {
                    chainedHeader = default(ChainedHeader);
                    return false;
                }
            }
        }

        public bool TryRemoveChainedHeader(UInt256 blockHash)
        {
            using (var txn = this.jetInstance.BeginTransaction())
            {
                var key = DbEncoder.EncodeUInt256(blockHash);
                if (txn.ContainsKey(blockHeadersTableId, key))
                {
                    txn.Delete(blockHeadersTableId, key);
                    txn.Commit();
                    cachedMaxHeader = null;
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

        private ChainedHeader cachedMaxHeader;
        public ChainedHeader FindMaxTotalWork()
        {
            if (cachedMaxHeader != null)
                return cachedMaxHeader;

            ChainedHeader maxHeader = null;

            foreach (var testHeader in ReadChainedHeaders())
            {
                if (maxHeader == null || testHeader.TotalWork > maxHeader.TotalWork)
                    maxHeader = testHeader;
            }

            cachedMaxHeader = maxHeader;
            return maxHeader;
        }

        public IEnumerable<ChainedHeader> ReadChainedHeaders()
        {
            using (var txn = this.jetInstance.BeginTransaction(TransactionBeginFlags.ReadOnly))
            using (var cursor = txn.CreateCursor(blockHeadersTableId))
            {
                var kvPair = cursor.MoveToFirst();
                while (kvPair != null)
                {
                    var chainedHeader = DataEncoder.DecodeChainedHeader(kvPair.Value.Value);
                    yield return chainedHeader;

                    kvPair = cursor.MoveNext();
                }
            }
        }

        public bool IsBlockInvalid(UInt256 blockHash)
        {
            using (var txn = this.jetInstance.BeginTransaction(TransactionBeginFlags.ReadOnly))
            {
                return txn.ContainsKey(invalidBlocksTableId, DbEncoder.EncodeUInt256(blockHash));
            }
        }

        public void MarkBlockInvalid(UInt256 blockHash)
        {
            using (var txn = this.jetInstance.BeginTransaction())
            {
                txn.Put(invalidBlocksTableId, DbEncoder.EncodeUInt256(blockHash), new byte[0]);
                txn.Commit();
            }
        }

        public int Count
        {
            get
            {
                using (var txn = this.jetInstance.BeginTransaction(TransactionBeginFlags.ReadOnly))
                {
                    return txn.GetEntriesCount(blockHeadersTableId).ToIntChecked();
                }
            }
        }

        public string Name
        {
            get { return "Blocks"; }
        }

        public bool TryRemove(UInt256 blockHash)
        {
            throw new NotImplementedException();
        }

        public void Flush()
        {
            this.jetInstance.Flush(force: true);
        }

        public void Defragment()
        {
            logger.Info("Block database: {0:#,##0} MB".Format2(this.jetInstance.UsedSize / 1.MILLION()));
        }
    }
}
