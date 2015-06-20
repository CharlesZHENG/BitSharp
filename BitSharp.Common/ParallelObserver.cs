using BitSharp.Common.ExtensionMethods;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Reactive;
using System.Reactive.Linq;

namespace BitSharp.Common
{
    public class ParallelObserver<T> : IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly string name;
        private readonly int consumerThreadCount;

        // the pool of worker threads where the source items will be consumed
        private readonly WorkerMethod[] consumeWorkers;

        // event to track when consuming has been completed
        private readonly ManualResetEventSlim completedConsumingEvent = new ManualResetEventSlim(false);

        // the source to observe
        private IObservable<T> source;

        // the observer to be notified by each consumer thread
        private IObserver<T> observer;

        // the queue of read items that are waiting to be consumed
        private ConcurrentBlockingQueue<T> queue;

        // track the number of items currently being processed
        private int processingCount;

        // track consumer work thread completions
        private int consumeCompletedCount;

        // any exceptions that occur during reading or consuming
        private ConcurrentBag<Exception> exceptions;

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
                this.consumeWorkers.DisposeList();
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
        public IDisposable SubscribeObservers(IObservable<T> source, IObserver<T> observer)
        {
            if (this.isStarted)
                throw new InvalidOperationException();

            // store the actions
            this.observer = observer;

            // initialize queue
            this.queue = new ConcurrentBlockingQueue<T>();
            this.source = source;

            // initialize exceptions and reset completed count
            this.exceptions = new ConcurrentBag<Exception>();
            this.processingCount = 0;
            this.consumeCompletedCount = 0;

            // set to the started state
            this.completedConsumingEvent.Reset();
            this.isStarted = true;
            this.exceptionsThrown = false;

            // notify the consume workers to begin
            for (var i = 0; i < this.consumeWorkers.Length; i++)
                this.consumeWorkers[i].NotifyWork();

            // begin observing
            return this.source
                .Finally(() =>
                {
                    this.queue.CompleteAdding();

                    Stop();
                })
                .Subscribe(
                    onNext: item =>
                    {
                        // queue the item to be consumed
                        this.queue.Add(item);
                    },
                    onError: ex =>
                    {
                        this.exceptions.Add(ex);
                    });
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
            this.completedConsumingEvent.Wait();

            // if any exceptions were thrown, rethrow them here
            if (this.exceptions.Count > 0)
            {
                this.exceptionsThrown = true;
                throw new AggregateException(this.exceptions);
            }
        }

        private void Stop()
        {
            if (!this.isStarted)
                throw new InvalidOperationException();

            // wait for the completed state
            this.completedConsumingEvent.Wait();

            // dispose the queue
            this.queue.Dispose();

            // capture any unthrown exceptions before clearing
            var unthrownExceptions = !exceptionsThrown ? this.exceptions : null;

            // null out all the fields used for this session
            this.source = null;
            this.observer = null;
            this.queue = null;
            this.exceptions = null;

            this.isStarted = false;

            // if any exceptions were unthrown, rethrow them here
            if (unthrownExceptions != null && unthrownExceptions.Count > 0)
                throw new AggregateException(unthrownExceptions);
        }

        private void ConsumeWorker(WorkerMethod instance)
        {
            // consume all read items
            try
            {
                foreach (var value in this.queue.GetConsumingEnumerable())
                {
                    // break early on an exception
                    if (this.exceptions.Count > 0)
                        return;

                    observer.OnNext(value);
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
                    if (this.exceptions.Count > 0)
                        observer.OnError(new AggregateException(this.exceptions));

                    observer.OnCompleted();

                    // notify that consuming has been completed
                    this.completedConsumingEvent.Set();
                }
            }
        }
    }
}
