using BitSharp.Common.ExtensionMethods;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Reactive;
using System.Reactive.Linq;
using System.Reactive.Disposables;
using System.Threading.Tasks;
using System.Diagnostics;

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

        private TaskCompletionSource<object> tcs;
        private TaskCompletionSource<object> readTcs;

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

        public Task SubscribeObservers(IEnumerable<T> source, IObserver<T> observer)
        {
            Task readTask;
            return SubscribeObservers(source, observer, out readTask);
        }

        /// <summary>
        /// Start a new parallel consuming session on the provided source, calling the provided actions.
        /// </summary>
        /// <param name="source">The source enumerable to read from.</param>
        /// <param name="consumeAction">The action to be called on each item from the source.</param>
        /// <param name="completedAction">An action to be called when all items have been successfully read and consumed. This will not be called if there is an exception.</param>
        /// <returns>An IDisposable to cleanup the parallel consuming session.</returns>
        public Task SubscribeObservers(IEnumerable<T> source, IObserver<T> observer, out Task readTask)
        {
            if (this.isStarted)
                throw new InvalidOperationException();

            this.tcs = new TaskCompletionSource<object>();
            this.readTcs = new TaskCompletionSource<object>();

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
            this.isStarted = true;

            // notify the read worker to begin
            this.readWorker.NotifyWork();

            // notify the consume workers to begin
            for (var i = 0; i < this.consumeWorkers.Length; i++)
                this.consumeWorkers[i].NotifyWork();

            // begin observing
            readTask = readTcs.Task;
            return tcs.Task;
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
                        break;

                    // queue the item to be consumed
                    this.queue.Add(item);
                }

                readTcs.SetResult(null);
            }
            // capture any thrown exceptions
            catch (Exception ex)
            {
                this.readException = ex;
                readTcs.SetException(ex);
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
                    // break early if an exception occurred
                    if (this.readException != null || this.consumeExceptions.Count > 0)
                        return;

                    observer.OnNext(value);
                }
            }
            // capture any thrown exceptions
            catch (Exception ex)
            {
                this.consumeExceptions.Add(ex);
            }
            // ensure consumer thread completion is tracked
            finally
            {
                // increment the completed consumer count and check if all have been completed
                if (Interlocked.Increment(ref this.consumeCompletedCount) == this.consumeWorkers.Length)
                {
                    this.completedReadingEvent.Wait();

                    var tcsLocal = this.tcs;
                    var observerLocal = this.observer;
                    var readExceptionLocal = this.readException;
                    var consumeExceptionsLocal = this.consumeExceptions;

                    this.queue.Dispose();

                    this.source = null;
                    this.observer = null;
                    this.queue = null;
                    this.consumeExceptions = null;

                    this.isStarted = false;

                    try
                    {
                        if (readExceptionLocal != null)
                            observerLocal.OnError(readExceptionLocal);
                        else if (consumeExceptionsLocal.Count > 0)
                            observerLocal.OnError(new AggregateException(consumeExceptionsLocal));
                        else
                            observerLocal.OnCompleted();
                    }
                    catch (Exception) { }


                    if (readExceptionLocal != null)
                        tcsLocal.SetException(readExceptionLocal);
                    else if (consumeExceptionsLocal.Count > 0)
                        tcsLocal.SetException(new AggregateException(consumeExceptionsLocal));
                    else
                        tcsLocal.SetResult(null);
                }
            }
        }
    }
}
