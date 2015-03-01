using System.Collections.Concurrent;
using System.Collections.Generic;

namespace BitSharp.Node.Storage.Memory
{
    public class MemoryStorage<TKey, TValue> : IBoundedStorage<TKey, TValue>
    {
        private readonly ConcurrentDictionary<TKey, TValue> storage = new ConcurrentDictionary<TKey, TValue>();

        public void Dispose()
        {
        }

        internal ConcurrentDictionary<TKey, TValue> Storage { get { return this.storage; } }

        public int Count
        {
            get { return this.storage.Count; }
        }

        public IEnumerable<TKey> Keys
        {
            get { return this.storage.Keys; }
        }

        public IEnumerable<TValue> Values
        {
            get { return this.storage.Values; }
        }

        public bool ContainsKey(TKey key)
        {
            return this.storage.ContainsKey(key);
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            return this.storage.TryGetValue(key, out value);
        }

        public bool TryAdd(TKey key, TValue value)
        {
            return this.storage.TryAdd(key, value);
        }

        public bool TryRemove(TKey key)
        {
            TValue ignore;
            return this.storage.TryRemove(key, out ignore);
        }

        public TValue this[TKey key]
        {
            get
            {
                return this.storage[key];
            }
            set
            {
                this.storage[key] = value;
            }
        }

        public void Flush()
        {
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return this.storage.GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
    }
}
