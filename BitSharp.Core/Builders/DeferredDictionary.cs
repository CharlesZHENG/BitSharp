using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Core.Builders
{
    public class DeferredDictionary<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private Dictionary<TKey, TValue> read = new Dictionary<TKey, TValue>();
        private HashSet<TKey> missing = new HashSet<TKey>();
        private Dictionary<TKey, TValue> updated = new Dictionary<TKey, TValue>();
        private Dictionary<TKey, TValue> added = new Dictionary<TKey, TValue>();
        private HashSet<TKey> deleted = new HashSet<TKey>();

        private readonly Func<TKey, Tuple<bool, TValue>> parentTryGetValue;
        private readonly Func<IEnumerable<KeyValuePair<TKey, TValue>>> parentEnumerator;

        private ConcurrentDictionary<TKey, TValue> parentValues = new ConcurrentDictionary<TKey, TValue>();

        public DeferredDictionary(Func<TKey, Tuple<bool, TValue>> parentTryGetValue, Func<IEnumerable<KeyValuePair<TKey, TValue>>> parentEnumerator = null)
        {
            this.parentTryGetValue = parentTryGetValue;
            this.parentEnumerator = parentEnumerator;
        }

        public IDictionary<TKey, TValue> Updated { get { return updated; } }

        public IDictionary<TKey, TValue> Added { get { return added; } }

        public ISet<TKey> Deleted { get { return deleted; } }

        public bool ContainsKey(TKey key)
        {
            if (!missing.Contains(key) && !deleted.Contains(key))
            {
                if (read.ContainsKey(key) || updated.ContainsKey(key) || added.ContainsKey(key))
                    return true;

                TValue value;
                if (TryGetParentValue(key, out value))
                {
                    read.Add(key, value);
                    return true;
                }
                else
                {
                    missing.Add(key);
                    return false;
                }
            }

            return false;
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            if (!missing.Contains(key) && !deleted.Contains(key))
            {
                if (read.TryGetValue(key, out value) || updated.TryGetValue(key, out value) || added.TryGetValue(key, out value))
                    return true;

                if (TryGetParentValue(key, out value))
                {
                    read.Add(key, value);
                    return true;
                }
                else
                {
                    missing.Add(key);
                    return false;
                }
            }

            value = default(TValue);
            return false;
        }

        public bool TryAdd(TKey key, TValue value)
        {
            if (missing.Contains(key))
            {
                missing.Remove(key);
                added.Add(key, value);
                return true;
            }
            else if (deleted.Contains(key))
            {
                deleted.Remove(key);
                updated.Add(key, value);
                return true;
            }
            else if (read.ContainsKey(key))
            {
                return false;
            }
            else if (!added.ContainsKey(key) && !updated.ContainsKey(key))
            {
                TValue existingValue;
                if (!TryGetParentValue(key, out existingValue))
                {
                    added.Add(key, value);
                    return true;
                }
                else
                {
                    read.Add(key, existingValue);
                    return false;
                }
            }
            else
                return false;
        }

        public bool TryRemove(TKey key)
        {
            TValue ignore;

            if (missing.Contains(key) || deleted.Contains(key))
            {
                return false;
            }
            else if (read.ContainsKey(key) || updated.ContainsKey(key) || added.ContainsKey(key) || TryGetParentValue(key, out ignore))
            {
                deleted.Add(key);
                read.Remove(key);
                updated.Remove(key);
                added.Remove(key);
                return true;
            }
            else
                return false;
        }

        public bool TryUpdate(TKey key, TValue value)
        {
            TValue ignore;

            if (missing.Contains(key) || deleted.Contains(key))
            {
                return false;
            }
            else if (read.ContainsKey(key))
            {
                Debug.Assert(!updated.ContainsKey(key));
                Debug.Assert(!added.ContainsKey(key));

                updated.Add(key, value);
                read.Remove(key);
                return true;
            }
            else if (updated.ContainsKey(key))
            {
                Debug.Assert(!read.ContainsKey(key));
                Debug.Assert(!added.ContainsKey(key));

                updated[key] = value;
                return true;
            }
            else if (added.ContainsKey(key))
            {
                Debug.Assert(!read.ContainsKey(key));
                Debug.Assert(!updated.ContainsKey(key));

                added[key] = value;
                return true;
            }
            else if (TryGetParentValue(key, out ignore))
            {
                updated[key] = value;
                return true;
            }
            else
                return false;
        }

        public void AddOrUpdate(TKey key, TValue value)
        {
            if (!ContainsKey(key))
            {
                if (!TryAdd(key, value))
                    throw new InvalidOperationException();
            }
            else
            {
                if (!TryUpdate(key, value))
                    throw new InvalidOperationException();
            }
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            if (parentEnumerator == null)
                throw new NotSupportedException();

            foreach (var kvPair in parentEnumerator())
            {
                Debug.Assert(!missing.Contains(kvPair.Key));
                Debug.Assert(!added.ContainsKey(kvPair.Key));

                TValue currentValue;
                if (deleted.Contains(kvPair.Key))
                {
                    continue;
                }
                else if (updated.TryGetValue(kvPair.Key, out currentValue))
                {
                    yield return new KeyValuePair<TKey, TValue>(kvPair.Key, currentValue);
                }
                else
                {
                    yield return kvPair;
                }
            }

            foreach (var kvPair in added)
                yield return kvPair;
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void WarmupValue(TKey key, Func<TValue> valueFunc)
        {
            parentValues.GetOrAdd(key, _ => valueFunc());
        }

        private bool TryGetParentValue(TKey key, out TValue value)
        {
            if (parentValues.TryGetValue(key, out value))
            {
                return value != null;
            }
            else
            {
                var result = parentTryGetValue(key);
                if (result.Item1)
                {
                    value = result.Item2;
                    parentValues.TryAdd(key, value);
                    return true;
                }
                else
                {
                    value = default(TValue);
                    parentValues.TryAdd(key, value);
                    return false;
                }
            }
        }
    }
}
