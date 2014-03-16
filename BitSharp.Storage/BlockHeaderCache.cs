﻿using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Data;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BitSharp.Storage
{
    public class BlockHeaderCache : BoundedCache<UInt256, BlockHeader>
    {
        private readonly CacheContext _cacheContext;

        public BlockHeaderCache(CacheContext cacheContext)
            : base("BlockHeaderCache", cacheContext.StorageContext.BlockHeaderStorage)
        {
            this._cacheContext = cacheContext;
        }

        public CacheContext CacheContext { get { return this._cacheContext; } }

        public IStorageContext StorageContext { get { return this.CacheContext.StorageContext; } }
    }
}
