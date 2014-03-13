﻿using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Storage.ExtensionMethods;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Storage
{
    public class BoundedCache<TKey, TValue> : UnboundedCache<TKey, TValue>
    {
        // known keys
        private ConcurrentSet<TKey> knownKeys;

        private readonly IBoundedStorage<TKey, TValue> _dataStorage;

        public BoundedCache(string name, IBoundedStorage<TKey, TValue> dataStorage, long maxFlushMemorySize, long maxCacheMemorySize, Func<TValue, long> sizeEstimator)
            : base(name, dataStorage, maxFlushMemorySize, maxCacheMemorySize, sizeEstimator)
        {
            this.knownKeys = new ConcurrentSet<TKey>();

            this._dataStorage = dataStorage;

            this.OnAddition += (key, value) => AddKnownKey(key);
            this.OnModification += (key, value) => AddKnownKey(key);
            this.OnRetrieved += (key, value) => AddKnownKey(key);
            this.OnMissing += key => RemoveKnownKey(key);

            // load existing keys from storage
            LoadKeyFromStorage();
        }

        public IBoundedStorage<TKey, TValue> DataStorage { get { return this._dataStorage; } }

        // get count of known items
        public int Count
        {
            get { return this.knownKeys.Count; }
        }

        public bool ContainsKey(TKey key)
        {
            return this.knownKeys.Contains(key);
        }

        // get all known item keys
        public IEnumerable<TKey> GetAllKeys()
        {
            return this.knownKeys;
        }

        // get all known item keys, reads everything from storage
        public IEnumerable<TKey> GetAllKeysFromStorage()
        {
            var returnedKeys = new HashSet<TKey>();

            foreach (var key in this._dataStorage.ReadAllKeys())
            {
                returnedKeys.Add(key);
                AddKnownKey(key);
                yield return key;
            }

            // ensure that any keys not returned from storage are returned as well, pending items
            var pendingKeys = this.knownKeys.Except(returnedKeys);
            foreach (var key in pendingKeys)
                yield return key;
        }

        public override void CreateValue(TKey key, TValue value)
        {
            if (this.knownKeys.TryAdd(key))
            {
                base.CreateValue(key, value);
            }
        }

        public override void UpdateValue(TKey key, TValue value)
        {
            this.knownKeys.TryAdd(key);
            base.UpdateValue(key, value);
        }

        public override bool TryGetValue(TKey key, out TValue value, bool saveInCache = true)
        {
            if (this.ContainsKey(key))
            {
                return base.TryGetValue(key, out value, saveInCache);
            }
            else
            {
                value = default(TValue);
                return false;
            }
        }

        public void FillCache()
        {
            foreach (var value in StreamAllValues())
            {
                var valueSize = this.sizeEstimator(value.Value);

                if (this.MemoryCacheSize + valueSize < this.MaxCacheMemorySize)
                {
                    CacheValue(value.Key, value.Value);
                }
                else
                {
                    break;
                }
            }
        }

        // get all values, reads everything from storage
        public IEnumerable<KeyValuePair<TKey, TValue>> StreamAllValues()
        {
            var keys = new HashSet<TKey>(this.GetAllKeys());
            var returnedKeys = new HashSet<TKey>();

            // return and track items from flush pending list
            // ensure a key is never returned twice in case modifications are made during the enumeration
            foreach (var key in keys)
            {
                TValue value;
                if (TryGetPendingValue(key, out value))
                {
                    returnedKeys.Add(key);
                    yield return new KeyValuePair<TKey, TValue>(key, value);
                }
            }

            // return items from storage, still ensuring a key is never returned twice
            // storage doesn't need to add to returnedKeys as storage items will always be returned uniquely
            foreach (var storageKeyPair in this.DataStorage.ReadAllValues())
            {
                if (!returnedKeys.Contains(storageKeyPair.Key))
                {
                    // make sure any keys found in storage become known
                    AddKnownKey(storageKeyPair.Key);

                    yield return storageKeyPair;
                }
            }
        }

        // clear all state and reload
        private void ClearKnownKeys()
        {
            // clear known keys
            this.knownKeys.Clear();

            // reload existing keys from storage
            LoadKeyFromStorage();
        }

        // load all existing keys from storage
        private void LoadKeyFromStorage()
        {
            var count = 0;
            foreach (var key in this.DataStorage.ReadAllKeys())
            {
                AddKnownKey(key);
                count++;
            }
            Debug.WriteLine("{0}: Finished loading from storage: {1:#,##0}".Format2(this.Name, count));
        }

        // add a key to the known list, fire event if new
        private void AddKnownKey(TKey key)
        {
            // add to the list of known keys
            var wasAdded = this.knownKeys.TryAdd(key);

            // fire addition event
            if (wasAdded)
            {
                RaiseOnAddition(key, default(TValue));
            }
        }

        // remove a key from the known list, fire event if deleted
        private void RemoveKnownKey(TKey key)
        {
            // remove from the list of known keys
            this.knownKeys.TryRemove(key);
        }
    }
}
