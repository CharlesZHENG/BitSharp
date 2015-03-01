using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core;
using NLog;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace BitSharp.Node.Storage
{
    public class BoundedCache<TKey, TValue> : IBoundedCache<TKey, TValue>
    {
        public event Action<TKey, TValue> OnAddition;
        public event Action<TKey, TValue> OnModification;
        public event Action<TKey> OnRemoved;
        public event Action<TKey> OnMissing;

        private readonly Logger logger;
        private readonly string name;
        private readonly IBoundedStorage<TKey, TValue> dataStorage;
        private readonly ConcurrentSet<TKey> knownKeys;
        private readonly ConcurrentSetBuilder<TKey> missingData;

        public BoundedCache(string name, IBoundedStorage<TKey, TValue> dataStorage, Logger logger)
        {
            this.logger = logger;
            this.name = name;
            this.dataStorage = dataStorage;
            this.knownKeys = new ConcurrentSet<TKey>();
            this.missingData = new ConcurrentSetBuilder<TKey>();

            // load existing keys from storage
            this.knownKeys.UnionWith(this.dataStorage.Keys);
            this.logger.Info("{0}: Finished loading from storage: {1:#,##0}".Format2(this.Name, this.Count));
        }

        public void Dispose()
        {
        }

        public string Name { get { return this.name; } }

        public ImmutableHashSet<TKey> MissingData { get { return this.missingData.ToImmutable(); } }

        public int Count
        {
            get { return this.dataStorage.Count; }
        }

        public IEnumerable<TKey> Keys
        {
            get { return this.dataStorage.Keys; }
        }

        public IEnumerable<TValue> Values
        {
            get { return this.dataStorage.Values; }
        }

        public bool ContainsKey(TKey key)
        {
            return this.knownKeys.Contains(key);
        }

        // try to get a value
        public bool TryGetValue(TKey key, out TValue value)
        {
            // look in storage
            if (this.dataStorage.TryGetValue(key, out value))
            {
                if (AddKnownKey(key))
                    RaiseOnAddition(key, value);

                return true;
            }
            else
            {
                if (RemoveKnownKey(key))
                    RaiseOnMissing(key);

                return false;
            }
        }

        public bool TryAdd(TKey key, TValue value)
        {
            var result = this.dataStorage.TryAdd(key, value);

            if (AddKnownKey(key) || result)
                RaiseOnAddition(key, value);

            return result;
        }

        public bool TryRemove(TKey key)
        {
            var result = this.dataStorage.TryRemove(key);

            this.missingData.Remove(key);

            if (RemoveKnownKey(key) || result)
                RaiseOnRemoved(key);

            return result;
        }

        public TValue this[TKey key]
        {
            get
            {
                TValue value;
                if (this.TryGetValue(key, out value))
                {
                    return value;
                }
                else
                {
                    this.missingData.Add(key);
                    throw new MissingDataException(key);
                }
            }
            set
            {
                this.dataStorage[key] = value;

                if (AddKnownKey(key))
                    RaiseOnAddition(key, value);
                else
                    RaiseOnModification(key, value);
            }
        }

        public void Flush()
        {
            this.dataStorage.Flush();
        }

        // add a key to the known list, fire event if new
        private bool AddKnownKey(TKey key)
        {
            this.missingData.Remove(key);

            // add to the list of known keys
            return this.knownKeys.TryAdd(key);
        }

        // remove a key from the known list, fire event if deleted
        private bool RemoveKnownKey(TKey key)
        {
            // remove from the list of known keys
            return this.knownKeys.TryRemove(key);
        }

        private void RaiseOnAddition(TKey key, TValue value)
        {
            var handler = this.OnAddition;
            if (handler != null)
                handler(key, value);
        }

        private void RaiseOnModification(TKey key, TValue value)
        {
            var handler = this.OnModification;
            if (handler != null)
                handler(key, value);
        }

        private void RaiseOnRemoved(TKey key)
        {
            var handler = this.OnRemoved;
            if (handler != null)
                handler(key);
        }

        private void RaiseOnMissing(TKey key)
        {
            var handler = this.OnMissing;
            if (handler != null)
                handler(key);
        }
    }
}
