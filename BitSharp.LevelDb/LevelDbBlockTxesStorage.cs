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
            return db.TryGet(new ReadOptions(), DbEncoder.EncodeBlockHashTxIndex(blockHash, 0), out value);
        }

        public void PruneElements(IEnumerable<KeyValuePair<UInt256, IEnumerable<int>>> blockTxIndices)
        {
            throw new NotImplementedException();

            //using (var handle = cursorCache.TakeItem())
            //{
            //    var cursor = handle.Item;

            //    foreach (var keyPair in blockTxIndices)
            //    {
            //        var blockHash = keyPair.Key;
            //        var txIndices = keyPair.Value;

            //        using (var jetTx = cursor.jetSession.BeginTransaction())
            //        {
            //            int blockIndex;
            //            if (!TryGetBlockIndex(cursor, blockHash, out blockIndex))
            //                continue;

            //            var pruningCursor = new MerkleTreePruningCursor(blockIndex, cursor);

            //            // prune the transactions
            //            foreach (var index in txIndices)
            //            {
            //                var cachedCursor = new CachedMerkleTreePruningCursor<BlockTxNode>(pruningCursor);
            //                MerkleTree.PruneNode(cachedCursor, index);
            //            }

            //            jetTx.CommitLazy();
            //        }
            //    }
            //}
        }

        public void DeleteElements(IEnumerable<KeyValuePair<UInt256, IEnumerable<int>>> blockTxIndices)
        {
            throw new NotImplementedException();

            //using (var handle = cursorCache.TakeItem())
            //{
            //    var cursor = handle.Item;

            //    foreach (var keyPair in blockTxIndices)
            //    {
            //        var blockHash = keyPair.Key;
            //        var txIndices = keyPair.Value;

            //        using (var jetTx = cursor.jetSession.BeginTransaction())
            //        {
            //            int blockIndex;
            //            if (!TryGetBlockIndex(cursor, blockHash, out blockIndex))
            //                continue;

            //            // prune the transactions
            //            foreach (var index in txIndices)
            //            {
            //                // remove transactions
            //                Api.JetSetCurrentIndex(cursor.jetSession, cursor.blocksTableId, "IX_BlockIndexTxIndex");
            //                Api.MakeKey(cursor.jetSession, cursor.blocksTableId, blockIndex, MakeKeyGrbit.NewKey);
            //                Api.MakeKey(cursor.jetSession, cursor.blocksTableId, index, MakeKeyGrbit.None);

            //                if (Api.TrySeek(cursor.jetSession, cursor.blocksTableId, SeekGrbit.SeekEQ))
            //                    Api.JetDelete(cursor.jetSession, cursor.blocksTableId);
            //            }

            //            jetTx.CommitLazy();
            //        }
            //    }
            //}
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
                //TODO
                return 0;
                //using (var handle = cursorCache.TakeItem())
                //{
                //    var cursor = handle.Item;

                //    return Api.RetrieveColumnAsInt32(cursor.jetSession, cursor.globalsTableId, cursor.blockCountColumnId).Value;
                //}
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
                }

                db.Write(new WriteOptions(), writeBatch);
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

                db.Write(new WriteOptions(), writeBatch);
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
    }
}
