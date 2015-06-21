using BitSharp.Common.ExtensionMethods;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Reactive;
using System.Reactive.Linq;
using System.Reactive.Disposables;

namespace BitSharp.Common
{
    public class ParallelObserver<T> : IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly string name;
        private readonly int consumerThreadCount;

        // the worker thread where the source will be read
        private WorkerMethod readWorker;

        // the pool of worker threads where the source items will be consumed
        private readonly WorkerMethod[] consumeWorkers;

        // events to track when reading and consuming have been completed
        private readonly ManualResetEventSlim completedReadingEvent = new ManualResetEventSlim(false);
        private readonly ManualResetEventSlim completedConsumingEvent = new ManualResetEventSlim(false);

        // the source to observe
        private IEnumerable<T> source;

        // the observer to be notified by each consumer thread
        private IObserver<T> observer;

        // the queue of read items that are waiting to be consumed
        private ConcurrentBlockingQueue<T> queue;

        // track the number of items currently being processed
        private int processingCount;

        // track consumer work thread completions
        private int consumeCompletedCount;

        // any exceptions that occur during reading or consuming
        private Exception readException;
        private ConcurrentBag<Exception> consumeExceptions;

        // whether a consuming session is started
        private bool isStarted;
        private bool exceptionsThrown;

        private bool isDisposed;

        /// <summary>
        /// Initialize a new ParallelConsumer with a fixed-sized pool of threads.
        /// </summary>
        /// <param name="name">The name of the instance.</param>
        /// <param name="consumerThreadCount">The number of consumer threads to create.</param>
        /// <param name="logger">A logger.</param>
        public ParallelObserver(string name, int consumerThreadCount)
        {
            this.name = name;
            this.consumerThreadCount = consumerThreadCount;

            // initialize the read worker
            this.readWorker = new WorkerMethod(name + ".ReadWorker", ReadWorker, initialNotify: false, minIdleTime: TimeSpan.Zero, maxIdleTime: TimeSpan.MaxValue);
            this.readWorker.Start();

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
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!this.isDisposed && disposing)
            {
                this.readWorker.Dispose();
                this.consumeWorkers.DisposeList();
                this.completedReadingEvent.Dispose();
                this.completedConsumingEvent.Dispose();
                if (this.queue != null)
                    this.queue.Dispose();

                this.isDisposed = true;
            }
        }

        /// <summary>
        /// The name of the instance.
        /// </summary>
        public string Name { get { return this.name; } }

        public int ConsumerThreadCount { get { return this.consumerThreadCount; } }

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
        public IDisposable SubscribeObservers(IEnumerable<T> source, IObserver<T> observer)
        {
            if (this.isStarted)
                throw new InvalidOperationException();

            // store the actions
            this.observer = observer;

            // initialize queue
            this.queue = new ConcurrentBlockingQueue<T>();
            this.source = source;

            // initialize exceptions and reset completed count
            this.readException = null;
            this.consumeExceptions = new ConcurrentBag<Exception>();
            this.processingCount = 0;
            this.consumeCompletedCount = 0;

            // set to the started state
            this.completedReadingEvent.Reset();
            this.completedConsumingEvent.Reset();
            this.isStarted = true;
            this.exceptionsThrown = false;

            // notify the read worker to begin
            this.readWorker.NotifyWork();

            // notify the consume workers to begin
            for (var i = 0; i < this.consumeWorkers.Length; i++)
                this.consumeWorkers[i].NotifyWork();

            // begin observing
            return Disposable.Create(Stop);
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
            if (this.readException != null)
            {
                this.exceptionsThrown = true;
                throw this.readException;
            }
            else if (this.consumeExceptions.Count > 0)
            {
                this.exceptionsThrown = true;
                throw new AggregateException(this.consumeExceptions);
            }
        }

        public void WaitToCompleteReading()
        {
            if (!this.isStarted)
                throw new InvalidOperationException();

            // wait for reading and consuming to completed
            this.completedReadingEvent.Wait();
        }

        public void WaitToCompleteConsuming()
        {
            if (!this.isStarted)
                throw new InvalidOperationException();

            // wait for reading and consuming to completed
            this.completedConsumingEvent.Wait();
        }

        private void Stop()
        {
            if (!this.isStarted)
                throw new InvalidOperationException();

            // wait for the completed state
            this.completedReadingEvent.Wait();
            this.completedConsumingEvent.Wait();

            // dispose the queue
            this.queue.Dispose();

            // capture any unthrown exceptions before clearing
            var unthrownReadException = !exceptionsThrown ? this.readException : null;
            var unthrownConsumeExceptions = !exceptionsThrown ? this.consumeExceptions : null;

            // null out all the fields used for this session
            this.source = null;
            this.observer = null;
            this.queue = null;
            this.consumeExceptions = null;

            this.isStarted = false;

            // if any exceptions were unthrown, rethrow them here
            if (unthrownReadException != null)
                throw unthrownReadException;
            else if (unthrownConsumeExceptions != null && unthrownConsumeExceptions.Count > 0)
                throw new AggregateException(unthrownConsumeExceptions);
        }

        private void ReadWorker(WorkerMethod instance)
        {
            // read all source items
            try
            {
                foreach (var item in this.source)
                {
                    // break early if an exception occurred
                    if (this.consumeExceptions.Count > 0)
                        return;

                    // queue the item to be consumed
                    this.queue.Add(item);
                }
            }
            // capture any thrown exceptions
            catch (Exception e)
            {
                this.readException = e;
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
                    // break early on an exception
                    if (this.readException != null || this.consumeExceptions.Count > 0)
                        return;

                    observer.OnNext(value);
                }
            }
            // capture any thrown exceptions
            catch (Exception e)
            {
                this.consumeExceptions.Add(e);
            }
            // ensure consumer thread completion is tracked
            finally
            {
                // increment the completed consumer count and check if all have been completed
                if (Interlocked.Increment(ref this.consumeCompletedCount) == this.consumeWorkers.Length)
                {
                    try
                    {
                        if (this.readException != null)
                            observer.OnError(readException);
                        if (this.consumeExceptions.Count > 0)
                            observer.OnError(new AggregateException(this.consumeExceptions));
                        else
                            observer.OnCompleted();
                    }
                    finally
                    {
                        // notify that consuming has been completed
                        this.completedConsumingEvent.Set();
                    }
                }
            }
        }
    }
}
