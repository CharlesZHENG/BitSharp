using System;
using System.Collections.Generic;

namespace BitSharp.Node.Storage
{
    public abstract class PassthroughBoundedCache<TKey, TValue> : PassthroughUnboundedCache<TKey, TValue>, IBoundedCache<TKey, TValue>
    {
        private readonly IBoundedCache<TKey, TValue> cache;
        private readonly Action<TKey, TValue> onAddition;
        private readonly Action<TKey, TValue> onModification;
        private readonly Action<TKey> onRemoved;

        public PassthroughBoundedCache(IBoundedCache<TKey, TValue> cache)
            : base(cache)
        {
            this.cache = cache;

            this.onAddition = (blockHash, TValue) => { var handler = this.OnAddition; if (handler != null) { handler(blockHash, TValue); } };
            this.onModification = (blockHash, TValue) => { var handler = this.OnModification; if (handler != null) { handler(blockHash, TValue); } };
            this.onRemoved = (blockHash) => { var handler = this.OnRemoved; if (handler != null) { handler(blockHash); } };

            this.cache.OnAddition += this.onAddition;
            this.cache.OnModification += this.OnModification;
            this.cache.OnRemoved += this.OnRemoved;
        }

        public override void Dispose()
        {
            this.cache.OnAddition -= this.onAddition;
            this.cache.OnModification -= this.OnModification;
            this.cache.OnRemoved -= this.OnRemoved;

            base.Dispose();
        }

        public event Action<TKey, TValue> OnAddition;
        public event Action<TKey, TValue> OnModification;
        public event Action<TKey> OnRemoved;

        public int Count { get { return this.cache.Count; } }
        public IEnumerable<TKey> Keys { get { return this.cache.Keys; } }
        public IEnumerable<TValue> Values { get { return this.cache.Values; } }
    }
}
