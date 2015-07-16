using BitSharp.Common.ExtensionMethods;
using NLog;
using System;
using System.Diagnostics;
using System.Threading;

namespace BitSharp.Common
{
    /// <summary>
    /// <para>A cache for IDiposable instances.</para>
    /// <para>The cache will create new instances as required, and will dispose instances which no longer fit in the cache.</para>
    /// <para>The cache has a maximum capacity, but more instances than this may be created. The capacity limits how many cached instances will be kept as they are returned.</para>
    /// </summary>
    /// <typeparam name="T">The type of object being cached, must be IDiposable.</typeparam>
    public class DisposableCache<T> : IDisposable where T : class, IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly Func<T> createFunc;
        private readonly Action<T> prepareAction;

        private readonly T[] cache;
        private readonly object cacheLock = new object();

        private readonly AutoResetEvent itemFreedEvent = new AutoResetEvent(false);

        private bool isDisposed;

        /// <summary>
        /// Initializes a new instance of DisposableCache.
        /// </summary>
        /// <param name="capacity">The maximum number of instances to cache.</param>
        /// <param name="createFunc">A function to create new instances. This must be null if dynamically creating new instances is not allowed.</param>
        /// <param name="prepareAction">An action to call on instances before they are returned to the cache. This may be null.</param>
        public DisposableCache(int capacity, Func<T> createFunc = null, Action<T> prepareAction = null)
        {
            this.cache = new T[capacity];
            this.createFunc = createFunc;
            this.prepareAction = prepareAction;
        }

        /// <summary>
        /// Releases all cached instances.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!this.isDisposed && disposing)
            {
                this.cache.DisposeList();
                this.itemFreedEvent.Dispose();

                this.isDisposed = true;
            }
        }

        /// <summary>
        /// The maximum capacity of the cache.
        /// </summary>
        public int Capacity
        {
            get { return this.cache.Length; }
        }

        /// <summary>
        /// Take an instance from the cache. If allowed, a new instance will be created if no cached instances are available, or an available instace will be waited for.
        /// </summary>
        /// <exception cref="InvalidOperationException">If no cached instance is available when creating new instances is disallowed.</exception>
        /// <returns>A handle to an instance of <typeparamref name="T"/>.</returns>
        public DisposeHandle<T> TakeItem()
        {
            return TakeItem(TimeSpan.MaxValue);
        }

        /// <summary>
        /// <para>Take an instance from the cache, with a timeout if no cached instances are available.</para>
        /// </summary>
        /// <param name="timeout">The timespan to wait for a cache instance to become available.</param>
        /// <exception cref="TimeoutException">If no cached instance became available before the timeout expired.</exception>
        /// <returns>A handle to an instance of <typeparamref name="T"/>.</returns>
        public DisposeHandle<T> TakeItem(TimeSpan timeout)
        {
            // track running time
            var stopwatch = Stopwatch.StartNew();

            while (true)
            {
                // try to take a cache instance
                var handle = this.TryTakeItem();

                // instance found, return it
                if (handle != null)
                    return handle;

                if (timeout < TimeSpan.Zero || timeout == TimeSpan.MaxValue)
                {
                    this.itemFreedEvent.WaitOne();
                }
                else
                {
                    // determine the amount of timeout remaining
                    var remaining = timeout - stopwatch.Elapsed;
                    
                    // if timeout is remaining, wait up to that amount of time for a new instance to become available
                    if (remaining.Ticks > 0)
                        this.itemFreedEvent.WaitOne(remaining);
                    // otherwise, throw a timeout exception
                    else
                        throw new TimeoutException();
                }
            }
        }

        /// <summary>
        /// Make an instance available to the cache. If the maximum capacity would be exceeded, the instance will be disposed instead of being cached.
        /// </summary>
        /// <param name="item">The instance to be cached.</param>
        public void CacheItem(T item)
        {
            // prepare the instance to be cached
            if (this.prepareAction != null)
                this.prepareAction(item);

            // attempt to return the instance to the cache
            var cached = false;
            lock (this.cacheLock)
            {
                for (var i = 0; i < this.cache.Length; i++)
                {
                    if (this.cache[i] == null)
                    {
                        this.cache[i] = item;
                        cached = true;
                        break;
                    }
                }
            }

            // if cached, notify that an instance became available
            if (cached)
                this.itemFreedEvent.Set();
            // otherwise, dispose the uncached instance as its lifetime has expired
            else
                item.Dispose();
        }

        private DisposeHandle<T> TryTakeItem()
        {
            // attempt to take an instance from the cache
            T item = null;
            lock (this.cacheLock)
            {
                for (var i = 0; i < this.cache.Length; i++)
                {
                    if (this.cache[i] != null)
                    {
                        item = this.cache[i];
                        this.cache[i] = null;
                        break;
                    }
                }
            }

            // if no instance was available, create a new one if allowed
            if (item == null && this.createFunc != null)
                item = this.createFunc();

            // if an instance was available, return it wrapped in DisposeHandle which will return the instance back to the cache
            if (item != null)
                return new DisposeHandle<T>(() => this.CacheItem(item), item);
            // no instance was available
            else
                return null;
        }
    }
}
