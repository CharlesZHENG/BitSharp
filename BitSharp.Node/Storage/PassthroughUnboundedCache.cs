using System;
using System.Collections.Immutable;

namespace BitSharp.Node.Storage
{
    public abstract class PassthroughUnboundedCache<TKey, TValue> : IUnboundedCache<TKey, TValue>
    {
        private readonly IUnboundedCache<TKey, TValue> cache;

        private readonly Action<TKey> onMissing;

        public PassthroughUnboundedCache(IUnboundedCache<TKey, TValue> cache)
        {
            this.cache = cache;
            
            this.onMissing = (blockHash) => { var handler = this.OnMissing; if (handler != null) { handler(blockHash); } };
            this.cache.OnMissing += this.OnMissing;
        }

        public virtual void Dispose()
        {
            this.cache.OnMissing -= this.onMissing;
            
            this.cache.Dispose();
        }

        private void HandleOnMossing(TKey key)
        {
            var handler = this.OnMissing;
            if (handler != null)
                handler(key);
        }

        public event Action<TKey> OnMissing;

        public string Name { get { return this.cache.Name; } }
        public ImmutableHashSet<TKey> MissingData { get { return this.cache.MissingData; } }

        public bool ContainsKey(TKey key) { return this.cache.ContainsKey(key); }
        public bool TryGetValue(TKey key, out TValue value) { return this.cache.TryGetValue(key, out value); }
        public bool TryAdd(TKey key, TValue value) { return this.cache.TryAdd(key, value); }
        public bool TryRemove(TKey key) { return this.cache.TryRemove(key); }
        public TValue this[TKey key] { get { return this.cache[key]; } set { this.cache[key] = value; } }
        public void Flush() { this.cache.Flush(); }
    }
}
