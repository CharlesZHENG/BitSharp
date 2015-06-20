using NLog;
using System;
using System.Collections.Generic;
using System.Threading;

namespace BitSharp.Common
{
    public class LookAhead<T> : IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly string name;

        // the worker thread where the source will be read
        private WorkerMethod readWorker;

        // event to track when reading has been completed
        private readonly ManualResetEventSlim completedReadingEvent = new ManualResetEventSlim(false);

        // the source enumerable to read
        private IEnumerable<T> source;

        private CancellationToken? cancelToken;

        // the queue of read items that are waiting to be returned
        private ConcurrentBlockingQueue<T> queue;

        // any exception that occurs during reading
        private Exception readException;

        private bool isDisposed;

        /// <summary>
        /// Initialize a new LookAhead.
        /// </summary>
        /// <param name="name">The name of the instance.</param>
        public LookAhead(string name)
        {
            this.name = name;

            // initialize the read worker
            this.readWorker = new WorkerMethod(name + ".ReadWorker", ReadWorker, initialNotify: false, minIdleTime: TimeSpan.Zero, maxIdleTime: TimeSpan.MaxValue);
            this.readWorker.Start();
        }

        /// <summary>
        /// Release all resources.
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

                this.isDisposed = true;
            }
        }

        /// <summary>
        /// The name of the instance.
        /// </summary>
        public string Name { get { return this.name; } }

        public IEnumerable<T> ReadAll(IEnumerable<T> source, CancellationToken? cancelToken = null)
        {
            // store the source enumerable and cancel token
            this.source = source;
            this.cancelToken = cancelToken;

            // set to the started state
            this.readException = null;
            this.completedReadingEvent.Reset();

            // initialize queue
            using (this.queue = new ConcurrentBlockingQueue<T>())
            {
                // notify the read worker to begin
                this.readWorker.NotifyWork();

                try
                {
                    foreach (var item in this.queue.GetConsumingEnumerable())
                    {
                        if (this.readException != null)
                            break;

                        yield return item;
                    }

                    completedReadingEvent.Wait();

                    if (this.readException != null)
                        throw readException;
                }
                finally
                {
                    // ensure read worker is always finished
                    completedReadingEvent.Wait();
                }
            }
        }

        private void ReadWorker(WorkerMethod instance)
        {
            // read all source items
            try
            {
                foreach (var item in this.source)
                {
                    // cooperative loop
                    if (this.cancelToken != null && this.cancelToken.Value.IsCancellationRequested)
                        break;

                    // queue the item to be returned
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
                this.completedReadingEvent.Set();
            }
        }
    }
}
