﻿using BitSharp.Common;
using BitSharp.Data;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BitSharp.Storage
{
    public class BlockCache : BoundedCache<UInt256, Block>
    {
        private readonly CacheContext _cacheContext;

        public BlockCache(CacheContext cacheContext)
            : base("BlockCache", cacheContext.BlockStorage)
        {
            this._cacheContext = cacheContext;

            //TODO keep this?
            //this.OnRetrieved += (blockHash, block) => this.CacheContext.TxKeyCache.CacheBlock(block);
        }

        public CacheContext CacheContext { get { return this._cacheContext; } }

        public IStorageContext StorageContext { get { return this.CacheContext.StorageContext; } }

        public override void CreateValue(UInt256 blockHash, Block block)
        {
            this.CacheContext.BlockHeaderCache.CreateValue(blockHash, block.Header);
            base.CreateValue(blockHash, block);

            this.CacheContext.TransactionCache.CacheBlock(block);
        }

        public override void UpdateValue(UInt256 blockHash, Block block)
        {
            this.CacheContext.BlockHeaderCache.UpdateValue(blockHash, block.Header);
            base.UpdateValue(blockHash, block);

            this.CacheContext.TransactionCache.CacheBlock(block);
        }
    }
}
