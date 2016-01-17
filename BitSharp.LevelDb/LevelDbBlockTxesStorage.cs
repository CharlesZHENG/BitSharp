using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage;
using LevelDB;
using NLog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace BitSharp.LevelDb
{
    public class LevelDbBlockTxesStorage : IBlockTxesStorage
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly string dbDirectory;
        private readonly DB db;

        private readonly object countLock = new object();

        private readonly byte[] COUNT_KEY = new byte[] { 0 };
        private readonly byte EXISTS_PREFIX = 1;

        private bool isDisposed;

        public LevelDbBlockTxesStorage(string baseDirectory, ulong? cacheSize, ulong? writeCacheSize)
        {
            dbDirectory = Path.Combine(baseDirectory, "BlockTxes");

            db = DB.Open(dbDirectory, cacheSize ?? 0, writeCacheSize ?? 0);
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
                db.Dispose();

                isDisposed = true;
            }
        }

        public bool ContainsBlock(UInt256 blockHash)
        {
            Slice value;
            return db.TryGet(ReadOptions.Default, MakeExistsKey(blockHash), out value);
        }

        public void PruneElements(IEnumerable<KeyValuePair<UInt256, IEnumerable<int>>> blockTxIndices)
        {
            using (var snapshot = db.GetSnapshot())
            {
                var readOptions = new ReadOptions { Snapshot = snapshot };
                using (var iterator = db.NewIterator(readOptions))
                {
                    foreach (var keyPair in blockTxIndices)
                    {
                        var blockHash = keyPair.Key;
                        var txIndices = keyPair.Value;

                        var pruningCursor = new LevelDbMerkleTreePruningCursor(blockHash, iterator);

                        // prune the transactions
                        foreach (var index in txIndices)
                        {
                            var cachedCursor = new CachedMerkleTreePruningCursor<BlockTxNode>(pruningCursor);
                            MerkleTree.PruneNode(cachedCursor, index);
                        }

                        var writeBatch = pruningCursor.CreateWriteBatch();
                        try
                        {
                            db.Write(new WriteOptions(), writeBatch);
                        }
                        finally
                        {
                            writeBatch.Dispose();
                        }
                    }
                }
            }
        }

        public void DeleteElements(IEnumerable<KeyValuePair<UInt256, IEnumerable<int>>> blockTxIndices)
        {
            var writeBatch = new WriteBatch();
            try
            {
                foreach (var keyPair in blockTxIndices)
                {
                    var blockHash = keyPair.Key;
                    var txIndices = keyPair.Value;

                    // prune the transactions
                    foreach (var index in txIndices)
                    {
                        writeBatch.Delete(DbEncoder.EncodeBlockHashTxIndex(blockHash, index));
                    }
                }

                db.Write(new WriteOptions(), writeBatch);
            }
            finally
            {
                writeBatch.Dispose();
            }
        }

        public bool TryReadBlockTransactions(UInt256 blockHash, out IEnumerator<BlockTx> blockTxes)
        {
            if (ContainsBlock(blockHash))
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
            if (ContainsBlock(blockHash))
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
            using (var snapshot = db.GetSnapshot())
            {
                var readOptions = new ReadOptions { Snapshot = snapshot };
                using (var iterator = db.NewIterator(readOptions))
                {
                    iterator.Seek(DbEncoder.EncodeBlockHashTxIndex(blockHash, 0));
                    while (iterator.Valid())
                    {
                        var key = iterator.Key().ToArray();

                        UInt256 iteratorBlockHash; int txIndex;
                        DbEncoder.DecodeBlockHashTxIndex(key, out iteratorBlockHash, out txIndex);

                        if (iteratorBlockHash != blockHash)
                            yield break;

                        var value = iterator.Value().ToArray();

                        var blockTxNode = DataDecoder.DecodeBlockTxNode(value);

                        if (blockTxNode.Pruned && requireTx)
                            throw new MissingDataException(blockHash);

                        yield return blockTxNode;

                        iterator.Next();
                    }
                }
            }
        }

        public bool TryGetTransaction(UInt256 blockHash, int txIndex, out BlockTx transaction)
        {
            Slice value;
            if (db.TryGet(new ReadOptions(), DbEncoder.EncodeBlockHashTxIndex(blockHash, txIndex), out value))
            {
                var blockTxNode = DataDecoder.DecodeBlockTxNode(value.ToArray());
                if (!blockTxNode.Pruned)
                {
                    transaction = blockTxNode.ToBlockTx();
                    return true;
                }
                else
                {
                    transaction = default(BlockTx);
                    return false;
                }
            }
            else
            {
                transaction = default(BlockTx);
                return false;
            }
        }

        public int BlockCount
        {
            get
            {
                Slice countSlice;
                if (db.TryGet(ReadOptions.Default, COUNT_KEY, out countSlice))
                    return countSlice.ToInt32();
                else
                    return 0;
            }
        }

        public string Name => "Blocks";

        public bool TryAddBlockTransactions(UInt256 blockHash, IEnumerable<EncodedTx> blockTxes)
        {
            if (ContainsBlock(blockHash))
                return false;

            var writeBatch = new WriteBatch();
            try
            {
                int txCount;
                using (var snapshot = db.GetSnapshot())
                {
                    var readOptions = new ReadOptions { Snapshot = snapshot };

                    var txIndex = 0;
                    foreach (var tx in blockTxes)
                    {
                        var key = DbEncoder.EncodeBlockHashTxIndex(blockHash, txIndex);

                        Slice existingValue;
                        if (db.TryGet(readOptions, key, out existingValue))
                            return false;

                        var blockTx = new BlockTx(txIndex, tx);
                        var value = DataEncoder.EncodeBlockTxNode(blockTx);

                        writeBatch.Put(key, value);

                        txIndex++;
                    }

                    txCount = txIndex;
                }

                return TryAddBlockInner(blockHash, txCount, writeBatch);
            }
            finally
            {
                writeBatch.Dispose();
            }

            return true;
        }

        public bool TryRemoveBlockTransactions(UInt256 blockHash)
        {
            if (!ContainsBlock(blockHash))
                return false;

            var writeBatch = new WriteBatch();
            try
            {
                using (var snapshot = db.GetSnapshot())
                {
                    var readOptions = new ReadOptions { Snapshot = snapshot };
                    using (var iterator = db.NewIterator(readOptions))
                    {
                        iterator.Seek(DbEncoder.EncodeBlockHashTxIndex(blockHash, 0));
                        while (iterator.Valid())
                        {
                            var key = iterator.Key().ToArray();

                            UInt256 iteratorBlockHash; int txIndex;
                            DbEncoder.DecodeBlockHashTxIndex(key, out iteratorBlockHash, out txIndex);

                            if (iteratorBlockHash != blockHash)
                                break;

                            writeBatch.Delete(key);

                            iterator.Next();
                        }
                    }
                }

                return TryRemoveBlockInner(blockHash, writeBatch);
            }
            finally
            {
                writeBatch.Dispose();
            }

            return true;
        }

        public void Flush()
        {
        }

        public void Defragment()
        {
        }

        private bool TryAddBlockInner(UInt256 blockHash, int blockTxesCount, WriteBatch writeBatch)
        {
            // lock all when performing the block write, so count update is thread-safe
            lock (countLock)
            {
                // get the current count
                int count;
                Slice countSlice;
                if (db.TryGet(ReadOptions.Default, COUNT_KEY, out countSlice))
                    count = countSlice.ToInt32();
                else
                    count = 0;

                // check if block exists before writing
                var existsKey = MakeExistsKey(blockHash);
                Slice existsSlice;
                if (!db.TryGet(ReadOptions.Default, existsKey, out existsSlice))
                {
                    // update count and add block existence key
                    writeBatch.Put(COUNT_KEY, count + 1);
                    writeBatch.Put(existsKey, blockTxesCount);

                    db.Write(WriteOptions.Default, writeBatch);

                    return true;
                }
                else
                    return false;
            }
        }

        private bool TryRemoveBlockInner(UInt256 blockHash, WriteBatch writeBatch)
        {
            // lock all when performing the block write, so count update is thread-safe
            lock (countLock)
            {
                // get the current count
                int count;
                Slice countSlice;
                if (db.TryGet(ReadOptions.Default, COUNT_KEY, out countSlice))
                    count = countSlice.ToInt32();
                else
                    count = 0;

                // check if block exists before writing
                var existsKey = MakeExistsKey(blockHash);
                Slice existsSlice;
                if (db.TryGet(ReadOptions.Default, existsKey, out existsSlice))
                {
                    // update count and remove block existence key
                    writeBatch.Put(COUNT_KEY, count - 1);
                    writeBatch.Delete(existsKey);

                    db.Write(WriteOptions.Default, writeBatch);

                    return true;
                }
                else
                    return false;
            }
        }

        private byte[] MakeExistsKey(UInt256 blockHash)
        {
            var key = new byte[33];
            key[0] = EXISTS_PREFIX;
            blockHash.ToByteArrayBE(key, 1);

            return key;
        }
    }
}
