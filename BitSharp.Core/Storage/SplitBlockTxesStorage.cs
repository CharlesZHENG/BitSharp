using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage;
using NLog;
using System;
using System.Collections.Generic;
using System.Linq;

namespace BitSharp.Core.Storage
{
    public class SplitBlockTxesStorage : IBlockTxesStorage
    {
        private readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly int splitCount;
        private readonly IBlockTxesStorage[] storages;

        public SplitBlockTxesStorage(int splitCount, Func<int, IBlockTxesStorage> createBlockTxesStorage)
        {
            this.splitCount = splitCount;

            this.storages = new IBlockTxesStorage[splitCount];
            try
            {
                for (var i = 0; i < this.storages.Length; i++)
                    this.storages[i] = createBlockTxesStorage(i);
            }
            catch (Exception)
            {
                this.storages.DisposeList();
                throw;
            }
        }

        public SplitBlockTxesStorage(string[] storageLocations, Func<string, IBlockTxesStorage> createBlockTxesStorage)
        {
            this.splitCount = storageLocations.Length;

            this.storages = new IBlockTxesStorage[splitCount];
            try
            {
                for (var i = 0; i < storageLocations.Length; i++)
                    this.storages[i] = createBlockTxesStorage(storageLocations[i]);
            }
            catch (Exception)
            {
                this.storages.DisposeList();
                throw;
            }
        }

        public void Dispose()
        {
            this.storages.DisposeList();
        }

        public int BlockCount
        {
            get
            {
                return this.storages.Sum(x => x.BlockCount);
            }
        }

        public bool ContainsBlock(UInt256 blockHash)
        {
            return GetStorage(blockHash).ContainsBlock(blockHash);
        }

        public bool TryAddBlockTransactions(UInt256 blockHash, IEnumerable<Transaction> blockTxes)
        {
            return GetStorage(blockHash).TryAddBlockTransactions(blockHash, blockTxes);
        }

        public bool TryGetTransaction(UInt256 blockHash, int txIndex, out Transaction transaction)
        {
            return GetStorage(blockHash).TryGetTransaction(blockHash, txIndex, out transaction);
        }

        public bool TryRemoveBlockTransactions(UInt256 blockHash)
        {
            return GetStorage(blockHash).TryRemoveBlockTransactions(blockHash);
        }

        public bool TryReadBlockTransactions(UInt256 blockHash, out IEnumerator<BlockTx> blockTxes)
        {
            return GetStorage(blockHash).TryReadBlockTransactions(blockHash, out blockTxes);
        }

        public void PruneElements(IEnumerable<KeyValuePair<UInt256, IEnumerable<int>>> blockTxIndices)
        {
            foreach (var keyPair in blockTxIndices)
            {
                var blockHash = keyPair.Key;
                var blockTxes = keyPair.Value;

                GetStorage(blockHash).PruneElements(new[] { new KeyValuePair<UInt256, IEnumerable<int>>(blockHash, blockTxes) });
            }
        }

        public void DeleteElements(IEnumerable<KeyValuePair<UInt256, IEnumerable<int>>> blockTxIndices)
        {
            foreach (var keyPair in blockTxIndices)
            {
                var blockHash = keyPair.Key;
                var blockTxes = keyPair.Value;

                GetStorage(blockHash).DeleteElements(new[] { new KeyValuePair<UInt256, IEnumerable<int>>(blockHash, blockTxes) });
            }
        }

        public void Flush()
        {
            foreach (var storage in storages)
                storage.Flush();
        }

        public void Defragment()
        {
            foreach (var storage in storages)
                storage.Defragment();
        }

        private IBlockTxesStorage GetStorage(UInt256 blockHash)
        {
            return this.storages[(int)(blockHash.ToBigInteger() % splitCount)];
        }
    }
}
