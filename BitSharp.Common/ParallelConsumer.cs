using BitSharp.Common.ExtensionMethods;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace BitSharp.Common
{
    /// <summary>
    /// <para>ParalleConsumer provides a fixed thread pool for reading an enumerable source on one
    /// thread, and queueing each item to be consumed on a separate threads.</para>
    /// </summary>
    /// <remarks>
    /// <para>This provides very similar functionality to using a Parallel.ForEach to queue up work, without the overhead.</para>
    /// <para>When making thousands of Parallel.ForEach calls per second, delays will be hit starting the tasks.</para>
    /// </remarks>
    /// <typeparam name="T">The type of the items to be read and consumed.</typeparam>
    public class ParallelConsumer<T> : IDisposable
    {
        private readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly string name;

        // the worker thread where the source will be read
        private WorkerMethod readWorker;

        // the pool of worker threads where the source items will be consumed
        private readonly WorkerMethod[] consumeWorkers;

        // events to track when reading and consuming have been completed
        private readonly ManualResetEventSlim completedReadingEvent = new ManualResetEventSlim(false);
        private readonly ManualResetEventSlim completedConsumingEvent = new ManualResetEventSlim(false);

        // the source enumerable to read
        private IEnumerable<T> source;

        // the action to perform on each item read from the source
        private Action<T> consumeAction;

        // an action to perform when all items have been read and consumed
        private Action completedAction;

        // the queue of read items that are waiting to be consumed
        private ConcurrentBlockingQueue<T> queue;
        private bool queueOwned;

        // track the number of items currently being processed
        private int processingCount;

        // track consumer work thread completions
        private int consumeCompletedCount;

        // any exceptions that occur during reading or consuming
        private ConcurrentBag<Exception> exceptions;

        // whether a consuming session is started
        private bool isStarted;

        private bool isDisposed;

        /// <summary>
        /// Initialize a new ParallelConsumer with a fixed-sized pool of threads.
        /// </summary>
        /// <param name="name">The name of the instance.</param>
        /// <param name="consumerThreadCount">The number of consumer threads to create.</param>
        /// <param name="logger">A logger.</param>
        public ParallelConsumer(string name, int consumerThreadCount)
        {
            this.name = name;

            // initialize a pool of consume workers
            this.consumeWorkers = new WorkerMethod[consumerThreadCount];
            for (var i = 0; i < this.consumeWorkers.Length; i++)
            {
                this.consumeWorkers[i] = new WorkerMethod(name + ".ConsumeWorker." + i, ConsumeWorker, initialNotify: false, minIdleTime: TimeSpan.Zero, maxIdleTime: TimeSpan.MaxValue);
                this.consumeWorkers[i].Start();
            }
        }

        /// <summary>
        /// Stop consuming and release all resources.
        /// </summary>
        public void Dispose()
        {
            if (this.isDisposed)
                return;

            this.consumeWorkers.DisposeList();
            if (this.readWorker != null)
                this.readWorker.Dispose();
            this.completedReadingEvent.Dispose();
            this.completedConsumingEvent.Dispose();
            if (this.queue != null)
                this.queue.Dispose();

            this.isDisposed = true;
        }

        /// <summary>
        /// The name of the instance.
        /// </summary>
        public string Name { get { return this.name; } }

        /// <summary>
        /// The number of pending items to consume, including any currently being consumed.
        /// </summary>
        public int PendingCount
        {
            get
            {
                return this.queue.Count + this.processingCount;
            }
        }

        /// <summary>
        /// Start a new parallel consuming session on the provided source, calling the provided actions.
        /// </summary>
        /// <param name="source">The source enumerable to read from.</param>
        /// <param name="consumeAction">The action to be called on each item from the source.</param>
        /// <param name="completedAction">An action to be called when all items have been successfully read and consumed. This will not be called if there is an exception.</param>
        /// <returns>An IDisposable to cleanup the parallel consuming session.</returns>
        public IDisposable Start(IEnumerable<T> source, Action<T> consumeAction, Action completedAction)
        {
            if (this.isStarted)
                throw new InvalidOperationException();

            // initialize the read worker
            if (this.readWorker == null)
            {
                this.readWorker = new WorkerMethod(name + ".ReadWorker", ReadWorker, initialNotify: false, minIdleTime: TimeSpan.Zero, maxIdleTime: TimeSpan.MaxValue);
                this.readWorker.Start();
            }

            // store the source enumerable and actions
            this.source = source;
            this.consumeAction = consumeAction;
            this.completedAction = completedAction;

            // initialize queue and exceptions and reset completed count
            this.queue = new ConcurrentBlockingQueue<T>();
            this.queueOwned = true;
            this.exceptions = new ConcurrentBag<Exception>();
            this.processingCount = 0;
            this.consumeCompletedCount = 0;

            // set to the started state
            this.completedReadingEvent.Reset();
            this.completedConsumingEvent.Reset();
            this.isStarted = true;

            // notify the read worker to begin
            this.readWorker.NotifyWork();

            // notify the consume workers to begin
            for (var i = 0; i < this.consumeWorkers.Length; i++)
                this.consumeWorkers[i].NotifyWork();

            // return the IDisposable to cleanup this session
            return new Stopper(this);
        }

        public IDisposable Start(ConcurrentBlockingQueue<T> queue, Action<T> consumeAction, Action completedAction)
        {
            if (this.isStarted)
                throw new InvalidOperationException();

            // store the source enumerable and actions
            this.source = null;
            this.consumeAction = consumeAction;
            this.completedAction = completedAction;

            // initialize queue and exceptions and reset completed count
            this.queue = queue;
            this.queueOwned = false;
            this.exceptions = new ConcurrentBag<Exception>();
            this.processingCount = 0;
            this.consumeCompletedCount = 0;

            // set to the started state
            this.completedReadingEvent.Set();
            this.completedConsumingEvent.Reset();
            this.isStarted = true;

            // notify the consume workers to begin
            for (var i = 0; i < this.consumeWorkers.Length; i++)
                this.consumeWorkers[i].NotifyWork();

            // return the IDisposable to cleanup this session
            return new Stopper(this);
        }

        /// <summary>
        /// Blocks until all reading and consuming has been finished.
        /// </summary>
        /// <exception cref="AggregateException">Thrown if any exceptions occurred during reading or consuming. Contains the thrown exceptions.</exception>
        public void WaitToComplete()
        {
            if (!this.isStarted)
                throw new InvalidOperationException();

            // wait for reading and consuming to completed
            this.completedReadingEvent.Wait();
            this.completedConsumingEvent.Wait();

            // if any exceptions were thrown, rethrow them here
            if (this.exceptions.Count > 0)
                throw new AggregateException(this.exceptions);
        }

        private void Stop()
        {
            if (!this.isStarted)
                throw new InvalidOperationException();

            // wait for the completed state
            this.isStarted = false;
            this.completedReadingEvent.Wait();
            this.completedConsumingEvent.Wait();

            // dispose the queue
            if (this.queueOwned)
                this.queue.Dispose();

            // null out all the fields used for this session
            this.source = null;
            this.consumeAction = null;
            this.completedAction = null;
            this.queue = null;
            this.exceptions = null;
        }

        private void ReadWorker(WorkerMethod instance)
        {
            // read all source items
            try
            {
                foreach (var item in this.source)
                {
                    // break early if stopped or if an exception occurred
                    if (!this.isStarted || this.exceptions.Count > 0)
                        return;

                    // queue the item to be consumed
                    this.queue.Add(item);
                }
            }
            // capture any thrown exceptions
            catch (Exception e)
            {
                this.exceptions.Add(e);
            }
            // ensure queue is marked as complete for adding once reading has finished
            finally
            {
                this.queue.CompleteAdding();

                // notify that reading has been completed
                this.completedReadingEvent.Set();
            }
        }

        private void ConsumeWorker(WorkerMethod instance)
        {
            // consume all read items
            try
            {
                foreach (var value in this.queue.GetConsumingEnumerable())
                {
                    // break early if stopped or if an exception occurred
                    if (!this.isStarted || this.exceptions.Count > 0)
                        return;

                    Interlocked.Increment(ref this.processingCount);
                    try
                    {
                        // consume the item
                        this.consumeAction(value);
                    }
                    finally
                    {
                        Interlocked.Decrement(ref this.processingCount);
                    }
                }
            }
            // capture any thrown exceptions
            catch (Exception e)
            {
                this.exceptions.Add(e);
            }
            // ensure consumer thread completion is tracked
            finally
            {
                // increment the completed consumer count and check if all have been completed
                if (Interlocked.Increment(ref this.consumeCompletedCount) == this.consumeWorkers.Length)
                {
                    // if no exceptions occurred, invoke the completed action
                    if (this.exceptions.Count == 0)
                        this.completedAction();

                    // notify that consuming has been completed
                    this.completedConsumingEvent.Set();
                }
            }
        }

        // IDisposable to initiate cleanup of a consuming session
        private sealed class Stopper : IDisposable
        {
            private readonly ParallelConsumer<T> worker;
            private bool isDisposed;

            public Stopper(ParallelConsumer<T> worker)
            {
                this.worker = worker;
            }

            public void Dispose()
            {
                if (this.isDisposed)
                    return;

                // stop the consuming session
                this.worker.Stop();

                this.isDisposed = true;
            }
        }
    }
}
