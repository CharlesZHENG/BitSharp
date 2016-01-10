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

        private const byte HEADER_PREFIX = 0;
        private const byte BLOCK_INVALID_PREFIX = 1;
        private const byte TOTAL_WORK_PREFIX = 2;

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
            var key = MakeHeaderKey(blockHash);

            Slice ignore;
            return db.TryGet(new ReadOptions(), key, out ignore);
        }

        public bool TryAddChainedHeader(ChainedHeader chainedHeader)
        {
            var key = MakeHeaderKey(chainedHeader.Hash);

            Slice existingValue;
            if (db.TryGet(ReadOptions.Default, key, out existingValue))
                return false;

            var writeBatch = new WriteBatch();
            try
            {
                writeBatch.Put(key, DataEncoder.EncodeChainedHeader(chainedHeader));
                writeBatch.Put(MakeTotalWorkKey(chainedHeader.Hash, chainedHeader.TotalWork), new byte[1]);

                db.Write(WriteOptions.Default, writeBatch);
            }
            finally
            {
                writeBatch.Dispose();
            }

            return true;
        }

        public bool TryGetChainedHeader(UInt256 blockHash, out ChainedHeader chainedHeader)
        {
            var key = MakeHeaderKey(blockHash);

            Slice value;
            if (db.TryGet(new ReadOptions(), key, out value))
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
            var key = MakeHeaderKey(blockHash);

            Slice existingValue;
            if (!db.TryGet(ReadOptions.Default, key, out existingValue))
                return false;

            var chainedHeader = DataDecoder.DecodeChainedHeader(existingValue.ToArray());

            var writeBatch = new WriteBatch();
            try
            {
                writeBatch.Delete(key);
                writeBatch.Delete(MakeTotalWorkKey(blockHash, chainedHeader.TotalWork));

                db.Write(WriteOptions.Default, writeBatch);
            }
            finally
            {
                writeBatch.Dispose();
            }

            return true;
        }

        public ChainedHeader FindMaxTotalWork()
        {
            var maxTotalWork = BigInteger.Zero;
            var candidateHeaders = new List<ChainedHeader>();

            using (var snapshot = db.GetSnapshot())
            {
                var readOptions = new ReadOptions { Snapshot = snapshot };

                using (var iterator = db.NewIterator(readOptions))
                {
                    // totalWork will be sorted lowest to highest
                    iterator.SeekToLast();
                    while (iterator.Valid())
                    {
                        var key = iterator.Key().ToArray();
                        if (key[0] != TOTAL_WORK_PREFIX)
                            break;

                        UInt256 blockHash;
                        BigInteger totalWork;
                        DecodeTotalWorkKey(key, out blockHash, out totalWork);

                        // check if this block is valid
                        bool isValid;
                        var blockInvalidKey = MakeBlockInvalidKey(blockHash);
                        Slice blockInvalidSlice;
                        if (db.TryGet(readOptions, blockInvalidKey, out blockInvalidSlice))
                        {
                            var blockInvalidBytes = blockInvalidSlice.ToArray();
                            isValid = !(blockInvalidBytes.Length == 1 && blockInvalidBytes[0] == 1);
                        }
                        else
                            isValid = true;

                        if (isValid)
                        {
                            var headerKey = MakeHeaderKey(blockHash);
                            Slice headerSlice;
                            if (db.TryGet(readOptions, headerKey, out headerSlice))
                            {
                                // decode chained header
                                var chainedHeader = DataDecoder.DecodeChainedHeader(headerSlice.ToArray());

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
                                else
                                    break;
                            }
                        }

                        iterator.Prev();
                    }
                }
            }

            // take the earliest header seen with the max total work
            candidateHeaders.Sort((left, right) => left.DateSeen.CompareTo(right.DateSeen));
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
                    while (iterator.Valid()
                        && iterator.Key().ToArray()[0] == HEADER_PREFIX)
                    {
                        var chainedHeader = DataDecoder.DecodeChainedHeader(iterator.Value().ToArray());
                        yield return chainedHeader;

                        iterator.Next();
                    }
                }
            }
        }

        public bool IsBlockInvalid(UInt256 blockHash)
        {
            var key = MakeBlockInvalidKey(blockHash);

            Slice blockInvalidSlice;
            if (db.TryGet(ReadOptions.Default, key, out blockInvalidSlice))
            {
                var blockInvalidBytes = blockInvalidSlice.ToArray();
                return blockInvalidBytes.Length == 1 && blockInvalidBytes[0] == 1;
            }
            else
                return false;
        }

        public void MarkBlockInvalid(UInt256 blockHash)
        {
            var key = MakeBlockInvalidKey(blockHash);

            db.Put(WriteOptions.Default, key, new byte[] { 1 });
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

        // TotalWork is stored in bytes as [prefix][totalWork big endian][blockHash big endian]
        // putting total work first ensures that entries are sorted by total work
        // including the blockHash ensures no coordination is needed to update the index
        private byte[] MakeTotalWorkKey(UInt256 blockHash, BigInteger totalWork)
        {
            if (totalWork < 0)
                throw new ArgumentOutOfRangeException(nameof(totalWork));

            var totalWorkBytes = totalWork.ToByteArray();
            if (totalWorkBytes.Length > 64)
                throw new ArgumentOutOfRangeException(nameof(totalWork));
            else if (totalWorkBytes.Length < 64)
                Array.Resize(ref totalWorkBytes, 64);

            Array.Reverse(totalWorkBytes);

            var key = new byte[1 + 64 + 32];
            key[0] = TOTAL_WORK_PREFIX;
            Buffer.BlockCopy(totalWorkBytes, 0, key, 1, 64);
            blockHash.ToByteArrayBE(key, 65);

            return key;
        }

        private void DecodeTotalWorkKey(byte[] key, out UInt256 blockHash, out BigInteger totalWork)
        {
            if (key.Length != 97)
                throw new ArgumentOutOfRangeException(nameof(key));
            else if (key[0] != TOTAL_WORK_PREFIX)
                throw new ArgumentOutOfRangeException(nameof(key));

            var totalWorkBytes = new byte[64];
            Buffer.BlockCopy(key, 1, totalWorkBytes, 0, 64);
            Array.Reverse(totalWorkBytes);
            totalWork = new BigInteger(totalWorkBytes);

            blockHash = UInt256.FromByteArrayBE(key, 65);
        }

        private byte[] MakeHeaderKey(UInt256 blockHash)
        {
            var key = new byte[33];
            key[0] = HEADER_PREFIX;
            blockHash.ToByteArrayBE(key, 1);

            return key;
        }

        private byte[] MakeBlockInvalidKey(UInt256 blockHash)
        {
            var key = new byte[33];
            key[0] = BLOCK_INVALID_PREFIX;
            blockHash.ToByteArrayBE(key, 1);

            return key;
        }

    }
}
