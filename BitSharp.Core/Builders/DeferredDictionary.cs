using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using NLog;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Core.Builders
{
    internal class DeferredDictionary<TKey, TValue> : IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private Dictionary<TKey, TValue> read = new Dictionary<TKey, TValue>();
        private HashSet<TKey> missing = new HashSet<TKey>();
        private Dictionary<TKey, TValue> updated = new Dictionary<TKey, TValue>();
        private Dictionary<TKey, TValue> added = new Dictionary<TKey, TValue>();
        private HashSet<TKey> deleted = new HashSet<TKey>();

        private readonly Func<TKey, Tuple<bool, TValue>> parentTryGetValue;

        private readonly ReaderWriterLockSlim warmupLock = new ReaderWriterLockSlim();

        public DeferredDictionary(Func<TKey, Tuple<bool, TValue>> parentTryGetValue)
        {
            this.parentTryGetValue = parentTryGetValue;
        }

        public void Dispose()
        {
            this.warmupLock.Dispose();
        }

        public IDictionary<TKey, TValue> Updated { get { return updated; } }

        public IDictionary<TKey, TValue> Added { get { return added; } }

        public ISet<TKey> Deleted { get { return deleted; } }

        public bool ContainsKey(TKey key)
        {
            return warmupLock.DoRead(() =>
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
            });
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            warmupLock.EnterReadLock();
            try
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
            finally
            {
                warmupLock.ExitReadLock();
            }
        }

        public bool TryAdd(TKey key, TValue value)
        {
            return warmupLock.DoRead(() =>
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
            });
        }

        public bool TryRemove(TKey key)
        {
            return warmupLock.DoRead(() =>
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
            });
        }

        public bool TryUpdate(TKey key, TValue value)
        {
            return warmupLock.DoRead(() =>
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
            });
        }

        public void WarmupValue(TKey key, Func<Tuple<bool, TValue>> getValue)
        {
            warmupLock.DoWrite(() =>
            {
                if (!read.ContainsKey(key) && !missing.Contains(key) && !updated.ContainsKey(key) && !added.ContainsKey(key) && !deleted.Contains(key))
                {
                    var value = getValue();
                    if (value.Item1)
                        read.Add(key, value.Item2);
                    else
                        missing.Add(key);
                }
            });
        }

        private static DurationMeasure getValueMeasure = new DurationMeasure(sampleCutoff: TimeSpan.FromMinutes(5));
        private bool TryGetParentValue(TKey key, out TValue value)
        {
            var stopwatch = Stopwatch.StartNew();
            var result = parentTryGetValue(key);
            stopwatch.Stop();

            if (result.Item1)
            {
                getValueMeasure.Tick(stopwatch.Elapsed);
                //Throttler.IfElapsed(TimeSpan.FromMilliseconds(50), () =>
                //{
                //    logger.Info("{0:N3}ms".Format2(getValueMeasure.GetAverage().TotalMilliseconds));
                //});

                value = result.Item2;
                return true;
            }
            else
            {
                value = default(TValue);
                return false;
            }
        }
    }
}
