using BitSharp.Common;
using BitSharp.Core;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage;
using LevelDB;
using NLog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;

namespace BitSharp.LevelDb
{
    public class LevelDbBlockStorage : IBlockStorage
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly string dbDirectory;
        private readonly string dbFile;
        private readonly DB db;

        private bool isDisposed;

        public LevelDbBlockStorage(string baseDirectory)
        {
            dbDirectory = Path.Combine(baseDirectory, "Blocks");
            dbFile = Path.Combine(dbDirectory, "Blocks.edb");

            db = DB.Open(dbFile);
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

        public bool ContainsChainedHeader(UInt256 blockHash)
        {
            Slice ignore;
            return db.TryGet(new ReadOptions(), DbEncoder.EncodeUInt256(blockHash), out ignore);
        }

        public bool TryAddChainedHeader(ChainedHeader chainedHeader)
        {
            if (ContainsChainedHeader(chainedHeader.Hash))
                return false;

            db.Put(new WriteOptions(), DbEncoder.EncodeUInt256(chainedHeader.Hash), DataEncoder.EncodeChainedHeader(chainedHeader));
            return true;
        }

        public bool TryGetChainedHeader(UInt256 blockHash, out ChainedHeader chainedHeader)
        {
            Slice value;
            if (db.TryGet(new ReadOptions(), DbEncoder.EncodeUInt256(blockHash), out value))
            {
                chainedHeader = DataDecoder.DecodeChainedHeader(value.ToArray());
                return true;
            }
            else
            {
                chainedHeader = default(ChainedHeader);
                return false;
            }
        }

        public bool TryRemoveChainedHeader(UInt256 blockHash)
        {
            if (!ContainsChainedHeader(blockHash))
                return false;

            db.Delete(new WriteOptions(), DbEncoder.EncodeUInt256(blockHash));
            return true;
        }

        //TODO
        public ChainedHeader FindMaxTotalWork()
        {
            logger.Info("finding max total work");
            var maxTotalWork = BigInteger.Zero;
            var candidateHeaders = new List<ChainedHeader>();

            using (var snapshot = db.GetSnapshot())
            {
                var readOptions = new ReadOptions { Snapshot = snapshot };

                using (var iterator = db.NewIterator(readOptions))
                {
                    iterator.SeekToFirst();
                    while (iterator.Valid())
                    {
                        // check if this block is valid
                        //TODO
                        var valid = true; // Api.RetrieveColumnAsBoolean(cursor.jetSession, cursor.blockHeadersTableId, cursor.blockHeaderValidColumnId);
                        if (valid)
                        {
                            // decode chained header
                            var chainedHeader = DataDecoder.DecodeChainedHeader(iterator.Value().ToArray());

                            // initialize max total work, if it isn't yet
                            if (maxTotalWork == BigInteger.Zero)
                                maxTotalWork = chainedHeader.TotalWork;

                            if (chainedHeader.TotalWork > maxTotalWork)
                            {
                                maxTotalWork = chainedHeader.TotalWork;
                                candidateHeaders = new List<ChainedHeader>();
                            }

                            // add this header as a candidate if it ties the max total work
                            if (chainedHeader.TotalWork >= maxTotalWork)
                                candidateHeaders.Add(chainedHeader);
                        }

                        iterator.Next();
                    }
                }
            }

            // take the earliest header seen with the max total work
            candidateHeaders.Sort((left, right) => left.DateSeen.CompareTo(right.DateSeen));
            logger.Info("finished finding max total work");
            return candidateHeaders.FirstOrDefault();
        }

        public IEnumerable<ChainedHeader> ReadChainedHeaders()
        {
            using (var snapshot = db.GetSnapshot())
            {
                var readOptions = new ReadOptions { Snapshot = snapshot };

                using (var iterator = db.NewIterator(readOptions))
                {
                    iterator.SeekToFirst();
                    while (iterator.Valid())
                    {
                        var chainedHeader = DataDecoder.DecodeChainedHeader(iterator.Value().ToArray());
                        yield return chainedHeader;

                        iterator.Next();
                    }
                }
            }
        }

        //TODO
        public bool IsBlockInvalid(UInt256 blockHash)
        {
            return false;
        }

        //TODO
        public void MarkBlockInvalid(UInt256 blockHash)
        {
            throw new NotImplementedException();
        }

        //TODO
        public int Count => 0;

        public string Name => "Blocks";

        public void Flush()
        {
        }

        public void Defragment()
        {
        }
    }
}
