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
    public class ParallelObservable<T> : IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly string name;

        // the worker thread where the source will be read
        private WorkerMethod readWorker;

        // events to track when reading and consuming have been completed
        private readonly ManualResetEventSlim completedReadingEvent = new ManualResetEventSlim(false);

        // the action to perform on a separate thread to produce the observable source
        private IObservable<T> source;

        // the queue of read items that are waiting to be consumed
        private ConcurrentBlockingQueue<T> queue;

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
        public ParallelObservable(string name)
        {
            this.name = name;

            // initialize the read worker
            this.readWorker = new WorkerMethod(name + ".ReadWorker", ReadWorker, initialNotify: false, minIdleTime: TimeSpan.Zero, maxIdleTime: TimeSpan.MaxValue);
            this.readWorker.Start();
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
                this.completedReadingEvent.Dispose();
                if (this.queue != null)
                    this.queue.Dispose();

                this.isDisposed = true;
            }
        }

        /// <summary>
        /// The name of the instance.
        /// </summary>
        public string Name { get { return this.name; } }

        /// <summary>
        /// Start a new parallel consuming session on the provided source, calling the provided actions.
        /// </summary>
        /// <param name="source">The source enumerable to read from.</param>
        /// <param name="consumeAction">The action to be called on each item from the source.</param>
        /// <param name="completedAction">An action to be called when all items have been successfully read and consumed. This will not be called if there is an exception.</param>
        /// <returns>An IDisposable to cleanup the parallel consuming session.</returns>
        public IObservable<T> Create(IObservable<T> source)
        {
            return Observable.Create(
                (Func<IObserver<T>, IDisposable>)(
                observer =>
                {
                    if (this.isStarted)
                        throw new InvalidOperationException();

                    // store the actions
                    this.source = source;

                    // initialize queue
                    this.queue = new ConcurrentBlockingQueue<T>();

                    // initialize exceptions and reset completed count
                    this.exceptions = new ConcurrentBag<Exception>();

                    // set to the started state
                    this.completedReadingEvent.Reset();
                    this.isStarted = true;
                    this.exceptionsThrown = false;

                    // notify the read worker to begin
                    this.readWorker.NotifyWork();

                    foreach (var item in this.queue.GetConsumingEnumerable())
                        observer.OnNext(item);

                    this.completedReadingEvent.Wait();
                    this.queue.Dispose();

                    if (this.exceptions.Count > 0)
                        observer.OnError(new AggregateException(this.exceptions));
                    else
                        observer.OnCompleted();

                    return Disposable.Create(() => this.isStarted = false);
                }));
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
        }

        private void ReadWorker(WorkerMethod instance)
        {
            // read all source items
            try
            {
                using (this.source.Subscribe(
                    onNext: item => this.queue.Add(item),
                    onError: ex => this.exceptions.Add(ex)))
                {
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
    }
}
