using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Common
{
    public class ConcurrentDictionaryBuilder<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>, IDictionary<TKey, TValue>
    {
        private readonly ImmutableDictionary<TKey, TValue>.Builder builder;
        private readonly object builderLock = new object();

        public ConcurrentDictionaryBuilder()
        {
            this.builder = ImmutableDictionary.CreateBuilder<TKey, TValue>();
        }

        public bool TryAdd(TKey key, TValue value)
        {
            lock (this.builderLock)
            {
                if (!this.builder.ContainsKey(key))
                {
                    this.builder.Add(key, value);
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

        public bool TryAdd(KeyValuePair<TKey, TValue> item)
        {
            lock (this.builderLock)
            {
                if (!this.builder.ContainsKey(item.Key))
                {
                    this.builder.Add(item);
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

        public bool TryRemove(TKey key, out TValue value)
        {
            lock (this.builderLock)
            {
                if (this.builder.TryGetValue(key, out value))
                {
                    this.builder.Remove(key);
                    return true;
                }
                else
                    return false;
            }
        }

        public void Add(TKey key, TValue value)
        {
            lock (this.builderLock)
                this.builder.Add(key, value);
        }

        public bool ContainsKey(TKey key)
        {
            lock (this.builderLock)
                return this.builder.ContainsKey(key);
        }

        public ICollection<TKey> Keys
        {
            get
            {
                return this.ToImmutable().Keys.ToImmutableList();
            }
        }

        public bool Remove(TKey key)
        {
            lock (this.builderLock)
                return this.builder.Remove(key);
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            lock (this.builderLock)
                return this.builder.TryGetValue(key, out value);
        }

        public ICollection<TValue> Values
        {
            get
            {
                return this.ToImmutable().Values.ToImmutableList();
            }
        }

        public TValue this[TKey key]
        {
            get
            {
                lock (this.builderLock)
                    return this.builder[key];
            }
            set
            {
                lock (this.builderLock)
                    this.builder[key] = value;
            }
        }

        public void Add(KeyValuePair<TKey, TValue> item)
        {
            lock (this.builderLock)
                this.builder.Add(item);
        }

        public void Clear()
        {
            lock (this.builderLock)
                this.builder.Clear();
        }

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            lock (this.builderLock)
                return this.builder.Contains(item);
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            var keyPairs = this.ToImmutable().ToArray();
            Buffer.BlockCopy(keyPairs, 0, array, arrayIndex, keyPairs.Length);
        }

        public int Count
        {
            get
            {
                lock (this.builderLock)
                    return this.builder.Count;
            }
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            lock (this.builderLock)
                return this.Remove(item);
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return this.ToImmutable().GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public ImmutableDictionary<TKey, TValue> ToImmutable()
        {
            lock (this.builderLock)
                return this.builder.ToImmutable();
        }
    }
}
