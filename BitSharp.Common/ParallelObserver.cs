using BitSharp.Common.ExtensionMethods;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Common
{
    public class ParallelObserver<T> : IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly string name;
        private readonly int consumerThreadCount;
        private readonly ReaderWriterLockSlim controlLock = new ReaderWriterLockSlim();

        // the pool of worker threads where the source items will be consumed
        private readonly WorkerMethod[] consumeWorkers;

        // the source to observe
        private IParallelReader<T> source;

        // the observer to be notified by each consumer thread
        private IObserver<T> observer;

        // track the number of items currently being processed
        private int processingCount;

        // track consumer work thread completions
        private int consumeCompletedCount;

        // any exceptions that occur during reading or consuming
        private Exception readException;
        private ConcurrentBag<Exception> consumeExceptions;

        private TaskCompletionSource<object> tcs;

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
                this.controlLock.Dispose();

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
                return controlLock.DoRead(() =>
                    (source != null ? source.Count : 0) + this.processingCount);
            }
        }

        /// <summary>
        /// Start a new parallel consuming session on the provided source, calling the provided actions.
        /// </summary>
        /// <param name="source">The source enumerable to read from.</param>
        /// <param name="consumeAction">The action to be called on each item from the source.</param>
        /// <param name="completedAction">An action to be called when all items have been successfully read and consumed. This will not be called if there is an exception.</param>
        /// <returns>An IDisposable to cleanup the parallel consuming session.</returns>
        public Task SubscribeObservers(IParallelReader<T> source, IObserver<T> observer)
        {
            controlLock.EnterWriteLock();
            try
            {
                if (this.tcs != null)
                    throw new InvalidOperationException();

                this.tcs = new TaskCompletionSource<object>();

                // store the actions
                this.observer = observer;

                // initialize queue
                this.source = source;

                // initialize exceptions and reset completed count
                this.readException = null;
                this.consumeExceptions = new ConcurrentBag<Exception>();
                this.processingCount = 0;
                this.consumeCompletedCount = 0;

                // notify the consume workers to begin
                for (var i = 0; i < this.consumeWorkers.Length; i++)
                    this.consumeWorkers[i].NotifyWork();

                // begin observing
                return tcs.Task;
            }
            finally
            {
                controlLock.ExitWriteLock();
            }
        }

        private void ConsumeWorker(WorkerMethod instance)
        {
            // consume all read items
            var wasConsumerEx = false;
            try
            {
                foreach (var value in this.source.GetConsumingEnumerable())
                {
                    // break early if an exception occurred
                    if (this.readException != null || this.consumeExceptions.Count > 0)
                        break;

                    wasConsumerEx = true;
                    observer.OnNext(value);
                    wasConsumerEx = false;
                }
            }
            // capture any thrown exceptions
            catch (Exception ex)
            {
                if (wasConsumerEx)
                    this.consumeExceptions.Add(ex);
                else
                    this.readException = ex;
                this.source.Cancel();
            }
            // ensure consumer thread completion is tracked
            finally
            {
                // increment the completed consumer count and check if all have been completed
                if (Interlocked.Increment(ref this.consumeCompletedCount) == this.consumeWorkers.Length)
                {
                    controlLock.EnterWriteLock();
                    try
                    {
                        try
                        {
                            if (readException != null)
                                observer.OnError(readException);
                            else if (consumeExceptions.Count > 0)
                                observer.OnError(new AggregateException(consumeExceptions));
                            else
                                observer.OnCompleted();
                        }
                        catch (Exception) { }

                        if (readException != null)
                            tcs.SetException(readException);
                        else if (consumeExceptions.Count > 0)
                            tcs.SetException(new AggregateException(consumeExceptions));
                        else
                            tcs.SetResult(null);

                        this.source = null;
                        this.observer = null;
                        this.readException = null;
                        this.consumeExceptions = null;
                        this.processingCount = 0;

                        this.tcs = null;
                    }
                    finally
                    {
                        controlLock.ExitWriteLock();
                    }
                }
            }
        }
    }
}
