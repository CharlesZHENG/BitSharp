using System;
using System.Collections.Generic;

namespace BitSharp.Common
{
    public class SortedValueDictionary<TKey, TValue> : IDictionary<TKey, TValue>
        where TValue : IComparable<TValue>
    {
        private readonly Dictionary<TKey, TValue> keys = new Dictionary<TKey, TValue>();
        private readonly SortedSet<TValue> values = new SortedSet<TValue>();

        public void Add(TKey key, TValue value)
        {
            if (!this.keys.ContainsKey(key))
            {
                this.keys.Add(key, value);
                this.values.Add(value);
            }
        }

        public bool ContainsKey(TKey key)
        {
            return this.keys.ContainsKey(key);
        }

        public ICollection<TKey> Keys => this.keys.Keys;

        public bool Remove(TKey key)
        {
            TValue value;
            if (this.keys.TryGetValue(key, out value))
            {
                this.keys.Remove(key);
                this.values.Remove(value);
                return true;
            }
            else
            {
                return false;
            }
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            return this.keys.TryGetValue(key, out value);
        }

        public ICollection<TValue> Values => this.values;

        public TValue this[TKey key]
        {
            get
            {
                return this.keys[key];
            }
            set
            {
                this.Remove(key);
                this.Add(key, value);
            }
        }

        public void Add(KeyValuePair<TKey, TValue> item)
        {
            this.Add(item.Key, item.Value);
        }

        public void Clear()
        {
            this.keys.Clear();
            this.values.Clear();
        }

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            TValue value;
            if (this.keys.TryGetValue(item.Key, out value))
            {
                return value.CompareTo(item.Value) == 0;
            }
            else
            {
                return false;
            }
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            throw new NotImplementedException();
        }

        public int Count => this.keys.Count;

        public bool IsReadOnly => false;

        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            if (this.Contains(item))
            {
                return this.Remove(item.Key);
            }
            else
            {
                return false;
            }
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            throw new NotImplementedException();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
    }
}
