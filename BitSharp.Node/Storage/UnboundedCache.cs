using BitSharp.Common;
using BitSharp.Core;
using System;
using System.Collections.Immutable;

namespace BitSharp.Node.Storage
{
    public class UnboundedCache<TKey, TValue> : IUnboundedCache<TKey, TValue>
    {
        private readonly string name;
        private readonly IUnboundedStorage<TKey, TValue> dataStorage;
        private readonly ConcurrentSetBuilder<TKey> missingData;

        public UnboundedCache(string name, IUnboundedStorage<TKey, TValue> dataStorage)
        {
            this.name = name;
            this.dataStorage = dataStorage;
            this.missingData = new ConcurrentSetBuilder<TKey>();
        }

        public void Dispose()
        {
            this.dataStorage.Dispose();
        }

        public event Action<TKey> OnMissing;

        public string Name { get { return this.name; } }

        public ImmutableHashSet<TKey> MissingData { get { return this.missingData.ToImmutable(); } }

        public bool ContainsKey(TKey key)
        {
            return this.dataStorage.ContainsKey(key);
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            if (this.dataStorage.TryGetValue(key, out value))
            {
                this.missingData.Remove(key);
                return true;
            }
            else
            {
                return false;
            }
        }

        public bool TryAdd(TKey key, TValue value)
        {
            this.missingData.Remove(key);
            return this.dataStorage.TryAdd(key, value);
        }

        public bool TryRemove(TKey key)
        {
            this.missingData.Remove(key);
            return this.dataStorage.TryRemove(key);
        }

        public TValue this[TKey key]
        {
            get
            {
                TValue value;
                if (this.dataStorage.TryGetValue(key, out value))
                {
                    this.missingData.Remove(key);
                    return value;
                }
                else
                {
                    this.missingData.Add(key);

                    var handler = this.OnMissing;
                    if (handler != null)
                        handler(key);

                    throw new MissingDataException(key);
                }
            }
            set
            {
                this.dataStorage[key] = value;
                this.missingData.Remove(key);
            }
        }

        public void Flush()
        {
            this.dataStorage.Flush();
        }
    }
}
