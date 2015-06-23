using BitSharp.Common.ExtensionMethods;
using NLog;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Common
{
    public class ParallelReader<T> : IParallelReader<T>, IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly string name;

        private readonly ReaderWriterLockSlim controlLock = new ReaderWriterLockSlim();

        // the worker thread where the source will be read
        private WorkerMethod readWorker;

        private IEnumerable<T> source;

        // the queue of read items that are waiting to be consumed
        private ConcurrentBlockingQueue<T> queue;

        // any exceptions that occur during reading
        private Exception readException;

        private TaskCompletionSource<object> tcs;
        private TaskCompletionSource<object> readsQueuedTcs;
        private CancellationTokenSource cancelToken;
        private CancellationTokenSource internalCancelToken;
        private CountdownEvent consumeStartedEvent;
        private CountdownEvent consumersCompleted;

        private bool isDisposed;

        public ParallelReader(string name)
        {
            this.name = name;

            // initialize the read worker
            this.readWorker = new WorkerMethod(name + ".ReadWorker", ReadWorker, initialNotify: false, minIdleTime: TimeSpan.Zero, maxIdleTime: TimeSpan.MaxValue);
            this.readWorker.Start();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!this.isDisposed && disposing)
            {
                CleanupPendingRead();
                this.readWorker.Dispose();
                this.controlLock.Dispose();

                this.isDisposed = true;
            }
        }

        public string Name { get { return this.name; } }

        public bool IsStarted
        {
            get
            {
                return controlLock.DoRead(() =>
                    this.tcs != null);
            }
        }

        public int Count
        {
            get
            {
                return controlLock.DoRead(() =>
                    queue != null ? queue.Count : 0);
            }
        }

        public Task ReadAsync(IEnumerable<T> source, CancellationToken? cancelToken = null)
        {
            Task readsQueuedTask;
            return ReadAsync(source, cancelToken, out readsQueuedTask);
        }

        public Task ReadAsync(IEnumerable<T> source, CancellationToken? cancelToken, out Task readsQueuedTask)
        {
            controlLock.EnterWriteLock();
            try
            {
                CheckStopped();

                // store the actions
                this.source = source;

                // initialize queue
                this.queue = new ConcurrentBlockingQueue<T>();

                // initialize exceptions and reset completed count
                this.readException = null;

                // set to the started state
                this.tcs = new TaskCompletionSource<object>();
                this.readsQueuedTcs = new TaskCompletionSource<object>();
                if (cancelToken.HasValue)
                {
                    this.internalCancelToken = new CancellationTokenSource();
                    this.cancelToken = CancellationTokenSource.CreateLinkedTokenSource(this.internalCancelToken.Token, cancelToken.Value);
                }
                else
                {
                    this.cancelToken = new CancellationTokenSource();
                    this.internalCancelToken = null;
                }
                this.consumeStartedEvent = new CountdownEvent(1);
                this.consumersCompleted = new CountdownEvent(1);

                // notify the read worker to begin
                this.readWorker.NotifyWork();

                readsQueuedTask = this.readsQueuedTcs.Task;
                return this.tcs.Task;
            }
            finally
            {
                controlLock.ExitWriteLock();
            }
        }

        public void Cancel(Exception ex)
        {
            try
            {
                if (ex != null)
                    readException = ex;
                if (cancelToken != null)
                    cancelToken.Cancel();
            }
            catch (Exception) { }
        }

        //TODO pass in the associated task so that a stale GetConsumingEnumerable() call can be detected and thrown on
        public IEnumerable<T> GetConsumingEnumerable()
        {
            controlLock.EnterReadLock();
            try
            {
                // yield nothing if not started
                // the reader may have fully completed while threads are still connecting, so this is not considered an error condition
                if (tcs == null)
                    yield break;

                // signal that a new consumer has started
                IncrementConsumer();
                try
                {
                    foreach (var item in this.queue.GetConsumingEnumerable())
                    {
                        yield return item;

                        // throw on cancellation or reader exception
                        cancelToken.Token.ThrowIfCancellationRequested();
                        if (this.readException != null)
                            throw this.readException;
                    }

                    // throw on reader exception
                    if (this.readException != null)
                        throw this.readException;
                }
                finally
                {
                    // signal that a consumer has completed
                    DecrementConsumer();
                }
            }
            finally
            {
                controlLock.ExitReadLock();
            }
        }

        private void ReadWorker(WorkerMethod instance)
        {
            // read all source items
            try
            {
                foreach (var item in this.source)
                {
                    this.queue.Add(item);

                    if (this.cancelToken.IsCancellationRequested)
                        break;
                }
            }
            // capture any thrown exception
            catch (Exception e)
            {
                this.readException = e;
            }
            finally
            {
                controlLock.EnterUpgradeableReadLock();
                try
                {
                    this.queue.CompleteAdding();

                    // complete the task indicating that all reads have been queued
                    if (this.readException != null)
                        this.readsQueuedTcs.SetException(this.readException);
                    else if (this.cancelToken.IsCancellationRequested)
                        this.readsQueuedTcs.SetCanceled();
                    else
                        this.readsQueuedTcs.SetResult(null);

                    // wait for all consuming to complete, or for the read to be cancelled
                    WaitForConsumedOrCancelled();

                    // finish the task and cleanup
                    Finish();
                }
                finally
                {
                    controlLock.ExitUpgradeableReadLock();
                }
            }
        }

        private void CheckStarted()
        {
            Debug.Assert(controlLock.IsReadLockHeld || controlLock.IsUpgradeableReadLockHeld || controlLock.IsWriteLockHeld);

            if (tcs == null)
                throw new InvalidOperationException("{0} is not started.".Format2(this.name));
        }

        private void CheckStopped()
        {
            Debug.Assert(controlLock.IsReadLockHeld || controlLock.IsUpgradeableReadLockHeld || controlLock.IsWriteLockHeld);

            if (tcs != null)
                throw new InvalidOperationException("{0} is already started.".Format2(this.name));
        }

        // wait for all items to be consumed, or for the task to be cancelled
        private void WaitForConsumedOrCancelled()
        {
            Debug.Assert(controlLock.IsUpgradeableReadLockHeld);
            do
            {
                // wait for consuming to begin before waiting for consuming to complete
                try
                {
                    this.consumeStartedEvent.Wait(this.cancelToken.Token);
                }
                catch (OperationCanceledException)
                {
                    // if reading was cancelled, force the started state so that the task can complete even if no consumer was ever connected
                    ForceStarted();
                }

                // wait for all consumers to finish, even when cancelled, each consumer will check the cancellation token
                this.consumersCompleted.Wait();
            }
            // check if waiting should resume: there are still items on the queue and the task has not been cancelled
            while (ResumeWaiting());
        }

        // cleanup and set the task completion result
        private void Finish()
        {
            controlLock.EnterWriteLock();
            try
            {
                var isCancelled = this.cancelToken.IsCancellationRequested;

                // cleanup
                this.queue.Dispose();
                this.cancelToken.Dispose();
                if (this.internalCancelToken != null)
                    this.internalCancelToken.Dispose();
                this.consumeStartedEvent.Dispose();
                this.consumersCompleted.Dispose();

                // set task result
                if (readException != null)
                    tcs.SetException(readException);
                else if (isCancelled)
                    tcs.SetCanceled();
                else
                    tcs.SetResult(null);

                // reset to stopped state
                this.tcs = null;
            }
            finally
            {
                controlLock.ExitWriteLock();
            }
        }

        // force the consumer count into the started state, if it isn't already
        private void ForceStarted()
        {
            controlLock.EnterWriteLock();
            try
            {
                if (!consumeStartedEvent.IsSet)
                {
                    consumeStartedEvent.Signal();
                    // decrement the consumer count as no real consumer was connected
                    consumersCompleted.Signal();
                    Debug.Assert(consumersCompleted.IsSet);
                }
            }
            finally
            {
                controlLock.ExitWriteLock();
            }
        }

        // check if waiting should resume: there are still items on the queue and the task has not been cancelled
        private bool ResumeWaiting()
        {
            controlLock.EnterWriteLock();
            try
            {
                if (this.readException != null || this.cancelToken.IsCancellationRequested)
                    return false;
                // resume waiting if there are still items on the queue
                else if (this.queue.Count > 0)
                {
                    // if consumption was started, reset to the unstarted state
                    if (consumeStartedEvent.IsSet)
                    {
                        // no consumers can be connected since a write-lock has been taken,
                        // this will not conflict with Increment/DecrementConsumer()
                        consumeStartedEvent.Reset(1);
                        consumersCompleted.Reset(1);
                    }
                    else
                        Debug.Assert(consumersCompleted.CurrentCount == 1);

                    return true;
                }
                else
                    return false;
            }
            finally
            {
                controlLock.ExitWriteLock();
            }
        }

        // increment consumer count, when GetConsumingEnumerable() begin
        private void IncrementConsumer()
        {
            lock (consumersCompleted)
            {
                // if first consumer, only signal that a consumer has started, do not add to count as it starts at 1
                if (!consumeStartedEvent.IsSet)
                    consumeStartedEvent.Signal();
                // if all previous consumers have finished, reset count to 1
                else if (consumersCompleted.IsSet)
                    consumersCompleted.Reset(1);
                // otherwise, increase count
                else
                    consumersCompleted.AddCount();
            }
        }

        // decrement consumer count, when GetConsumingEnumerable() completes
        private void DecrementConsumer()
        {
            lock (consumersCompleted)
            {
                if (!consumersCompleted.IsSet)
                    consumersCompleted.Signal();
                else
                    // consumer count can't go negative
                    throw new InvalidOperationException();
            }
        }

        // attempt to cleanup any outstanding read during disposal, without causing Dispose() to throw errors
        private void CleanupPendingRead()
        {
            try
            {
                if (cancelToken != null)
                {
                    cancelToken.Cancel();
                    cancelToken.Dispose();
                }
            }
            catch (Exception) { }

            try
            {
                if (internalCancelToken != null)
                {
                    internalCancelToken.Cancel();
                    internalCancelToken.Dispose();
                }
            }
            catch (Exception) { }

            try
            {
                if (consumeStartedEvent != null)
                {
                    consumeStartedEvent.Reset(0);
                    consumeStartedEvent.Dispose();
                }
            }
            catch (Exception) { }

            try
            {
                if (consumersCompleted != null)
                {
                    consumersCompleted.Reset(0);
                    consumersCompleted.Dispose();
                }
            }
            catch (Exception) { }
        }
    }
}
