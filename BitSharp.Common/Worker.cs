using BitSharp.Common.ExtensionMethods;
using NLog;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace BitSharp.Common
{
    /// <summary>
    /// <para>The Worker class provides base functionality for executing work on a separate thread,
    /// providing the ability to start, stop and notify.</para>
    /// <para>Derived classes provide a WorkAction implementation that represents the work to be performed.</para>
    /// <para>Work is scheduled in a non-blocking fashion with a call to NotifyWork.</para>
    /// </summary>
    public abstract class Worker : IDisposable
    {
        // dispose timeout, the worker thread will be aborted if it does not stop in a timely fashion on Dispose
        private static readonly TimeSpan DISPOSE_STOP_TIMEOUT = TimeSpan.FromSeconds(10);

        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        // the WorkerLoop thread
        private readonly Thread workerThread;

        // the start event blocks the worker thread until it has been started
        private readonly ManualResetEventSlim startEvent;

        // the notify event blocks the worker thread until it has been notified, enforcing the minimum idle time
        private readonly AutoResetEvent notifyEvent;

        // the force notify event blocks the worker thread until work is forced, allowing the mimimum idle time to be bypassed
        private readonly AutoResetEvent forceNotifyEvent;

        // the idle event is set when no work is being performed, either when stopped or when waiting for a notification
        private readonly ManualResetEventSlim idleEvent;

        // lock object for control methods Start and Stop
        private readonly object controlLock = new object();

        private bool isStarted;
        private bool isAlive;
        private bool isDisposed;

        /// <summary>
        /// Initialize a worker in the unnotified state, with a minimum idle time of zero, and no maximum idle time.
        /// </summary>
        /// <param name="name">The name of the worker.</param>
        /// <param name="logger">A logger for the worker</param>
        public Worker(string name)
            : this(name, initialNotify: false, minIdleTime: TimeSpan.Zero, maxIdleTime: TimeSpan.MaxValue)
        { }

        /// <summary>
        /// Initialize a worker.
        /// </summary>
        /// <param name="name">The name of the worker.</param>
        /// <param name="initialNotify">Whether the worker starts notified. If true, work will be performed as soon as the worker is started.</param>
        /// <param name="minIdleTime">The minimum idle time for the worker.</param>
        /// <param name="maxIdleTime">The maximum idle time for the worker.</param>
        /// <param name="logger">A logger for the worker.</param>
        public Worker(string name, bool initialNotify, TimeSpan minIdleTime, TimeSpan maxIdleTime)
        {
            this.Name = name;
            this.MinIdleTime = minIdleTime;
            this.MaxIdleTime = maxIdleTime;

            this.workerThread = new Thread(WorkerLoop);
            this.workerThread.Name = "Worker.{0}.WorkerLoop".Format2(name);

            this.startEvent = new ManualResetEventSlim(false);
            this.notifyEvent = new AutoResetEvent(initialNotify);
            this.forceNotifyEvent = new AutoResetEvent(initialNotify);
            this.idleEvent = new ManualResetEventSlim(false);

            this.isStarted = false;
            this.isAlive = true;
            this.isDisposed = false;

            this.workerThread.Start();
        }

        /// <summary>
        /// This event is raised when the worker is notified of work.
        /// </summary>
        public event Action OnNotifyWork;

        /// <summary>
        /// This event is raised before the worker performs work.
        /// </summary>
        public event Action OnWorkStarted;

        /// <summary>
        /// This event is raised after the worker completes work.
        /// </summary>
        public event Action OnWorkFinished;

        /// <summary>
        /// This event is raised when the worker throws an exception.
        /// </summary>
        public event Action<Exception> OnWorkError;

        /// <summary>
        /// The name of the worker.
        /// </summary>
        public string Name { get; protected set; }

        /// <summary>
        /// The minimum amount of time that must elapse in between performing work.
        /// </summary>
        /// <remarks>
        /// The minimum idle time throttles how often a worker will perform work. A call to
        /// NotifyWork will always schedule work to be performed, the minimum idle time will only
        /// delay the action. ForceWork can be called to immediately schedule work to be performed,
        /// disregarding the minimum idle time.
        /// </remarks>
        public TimeSpan MinIdleTime { get; set; }

        /// <summary>
        /// The maximum amount of time that may elapse in between performing work.
        /// </summary>
        /// <remarks>
        /// The maximum idle time will cause the worker to perform work regardless of whether
        /// NotifyWork was called, after it has been idle for the specified amount of time.
        /// </remarks>
        public TimeSpan MaxIdleTime { get; set; }

        /// <summary>
        /// Whether the worker is currently started.
        /// </summary>
        public bool IsStarted { get { return this.isStarted; } }

        /// <summary>
        /// Start the worker.
        /// </summary>
        /// <remarks>
        /// If the worker is notified, work will be performed immediately. If it's not notified,
        /// work won't be performed until a notification has been received, or the maximum idle
        /// time has passed.
        /// </remarks>
        public void Start()
        {
            CheckDisposed();

            // take the control lock
            lock (this.controlLock)
            {
                if (!this.isStarted)
                {
                    // set the worker to the started stated
                    this.isStarted = true;

                    // invoke the start hook for the sub-class
                    this.SubStart();

                    // unblock the worker loop
                    this.startEvent.Set();
                }
            }
        }

        /// <summary>
        /// Start the worker in the notified state.
        /// </summary>
        public void NotifyAndStart()
        {
            NotifyWork();
            Start();
        }

        /// <summary>
        /// Stop the worker, and wait for it to idle.
        /// </summary>
        /// <param name="timeout">The amount of time to wait for the worker to idle. If null, the worker will wait indefinitely.</param>
        /// <returns>True if the worker idled after being stopped, false if the timeout was reached without the worker idling.</returns>
        public bool Stop(TimeSpan? timeout = null)
        {
            CheckDisposed();

            // take the control lock
            lock (this.controlLock)
            {
                if (this.isStarted)
                {
                    // set the worker to the stopped state
                    this.isStarted = false;

                    // invoke the stop hook for the sub-class
                    this.SubStop();

                    // block the worker loop on the started event
                    this.startEvent.Reset();

                    // unblock the notify events to allow the worker loop to block on the started event
                    this.forceNotifyEvent.Set();
                    this.notifyEvent.Set();

                    // wait for the worker to idle
                    bool stopped;
                    if (timeout != null)
                    {
                        stopped = this.idleEvent.Wait(timeout.Value);
                        if (!stopped && timeout.Value > TimeSpan.Zero)
                            logger.Warn("Worker failed to stop: {0}".Format2(this.Name));
                    }
                    else
                    {
                        this.idleEvent.Wait();
                        stopped = true;
                    }

                    // reset the notify events after idling
                    if (stopped)
                    {
                        this.forceNotifyEvent.Reset();
                        this.notifyEvent.Reset();
                    }

                    return stopped;
                }
                else
                {
                    // worker is already stopped
                    return true;
                }
            }
        }

        /// <summary>
        /// Stop the worker and release all resources.
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
                // stop worker
                this.Stop(DISPOSE_STOP_TIMEOUT);

                // invoke the dispose hook for the sub-class
                this.SubDispose();

                // indicate that worker is no longer alive, and worker loop may exit
                this.isAlive = false;

                // unblock all events so that the worker loop can exit
                this.startEvent.Set();
                this.notifyEvent.Set();
                this.forceNotifyEvent.Set();

                // wait for the worker thread to exit
                if (this.workerThread.IsAlive)
                {
                    this.workerThread.Join(DISPOSE_STOP_TIMEOUT);

                    // terminate if still alive
                    if (this.workerThread.IsAlive)
                    {
                        logger.Warn("Worker thread aborted: {0}".Format2(this.Name));
                        this.workerThread.Abort();
                    }
                }

                // dispose events
                this.startEvent.Dispose();
                this.notifyEvent.Dispose();
                this.forceNotifyEvent.Dispose();
                this.idleEvent.Dispose();

                this.isDisposed = true;
            }
        }

        /// <summary>
        /// Notify the worker that work should be performed.
        /// </summary>
        /// <remarks>
        /// This method will ensure that work is always performed after being called. If the worker
        /// is currently performing work then another round of work will be performed afterwards,
        /// respecting the minimum idle time.
        /// </remarks>
        public void NotifyWork()
        {
            CheckDisposed();

            // unblock the notify event
            this.notifyEvent.Set();

            // raise the OnNotifyWork event
            var handler = this.OnNotifyWork;
            if (handler != null)
                handler();
        }

        /// <summary>
        /// Notify the worker that work should be performed immediately, disregarding the minimum idle time.
        /// </summary>
        /// <remarks>
        /// This method will ensure that work is always performed after being called. If the worker
        /// is currently performing work then another round of work will be performed immediately
        /// afterwards.
        /// </remarks>
        public void ForceWork()
        {
            CheckDisposed();

            // take the control lock
            lock (this.controlLock)
            {
                // invoke the force work hook for the sub-class
                this.SubForceWork();

                // unblock the force notify and notify events
                this.forceNotifyEvent.Set();
                this.notifyEvent.Set();
            }

            var handler = this.OnNotifyWork;
            if (handler != null)
                handler();
        }

        /// <summary>
        /// This method to invoke to perform work, must be implemented by the sub-class.
        /// </summary>
        protected abstract void WorkAction();

        /// <summary>
        /// An optional hook that will be called when the worker is diposed.
        /// </summary>
        protected virtual void SubDispose() { }

        /// <summary>
        /// An optional hook that will be called when the worker is started.
        /// </summary>
        protected virtual void SubStart() { }

        /// <summary>
        /// An optional hook that will be called when the worker is stopped.
        /// </summary>
        protected virtual void SubStop() { }

        /// <summary>
        /// An optional hook that will be called when the worker is forced to work.
        /// </summary>
        protected virtual void SubForceWork() { }

        /// <summary>
        /// Throw an OperationCanceledException exception if the worker has been stopped.
        /// </summary>
        /// <remarks>
        /// Ths method allows the sub-class to cooperatively stop working when a stop has been requested.
        /// </remarks>
        protected void ThrowIfCancelled()
        {
            CheckDisposed();

            if (!this.isStarted)
                throw new OperationCanceledException();
        }

        private void CheckDisposed()
        {
            if (this.isDisposed)
                throw new ObjectDisposedException("Worker access when disposed: {0}".Format2(this.Name));
        }

        private void WorkerLoop()
        {
            try
            {
                // stats
                var totalTime = Stopwatch.StartNew();
                var workerTime = new Stopwatch();
                var lastReportTime = DateTime.Now;

                // continue running as long as the worker is alive
                while (this.isAlive)
                {
                    // notify worker is idle
                    this.idleEvent.Set();

                    // wait for execution to start
                    this.startEvent.Wait();

                    // cooperative loop
                    if (!this.isStarted)
                        continue;

                    // delay for the requested wait time, unless forced
                    if (!this.forceNotifyEvent.WaitOne(this.MinIdleTime))
                    {
                        // wait for work notification
                        if (this.MaxIdleTime == TimeSpan.MaxValue)
                            this.notifyEvent.WaitOne();
                        else
                            this.notifyEvent.WaitOne(this.MaxIdleTime - this.MinIdleTime); // subtract time already spent waiting
                    }

                    // cooperative loop
                    if (!this.isStarted)
                        continue;

                    // notify that work is starting
                    this.idleEvent.Reset();

                    // notify work started
                    var startHandler = this.OnWorkStarted;
                    if (startHandler != null)
                        startHandler();

                    // perform the work
                    workerTime.Start();
                    try
                    {
                        WorkAction();
                    }
                    catch (Exception ex)
                    {
                        // ignore a cancellation exception
                        // workers can throw this to stop the current work action
                        bool cancelled;
                        if (ex is OperationCanceledException)
                            cancelled = true;
                        else if (ex is AggregateException
                                && ((AggregateException)ex).InnerExceptions.All(x => x is OperationCanceledException))
                            cancelled = true;
                        else
                            cancelled = false;

                        // worker leaked an exception
                        if (!cancelled)
                        {
                            logger.Error(ex, "Unhandled worker exception in {0}: ".Format2(this.Name));

                            // notify work error
                            var errorHandler = this.OnWorkError;
                            if (errorHandler != null)
                                errorHandler(ex);
                        }
                    }
                    finally
                    {
                        workerTime.Stop();
                    }

                    // notify work stopped
                    var stopHandler = this.OnWorkFinished;
                    if (stopHandler != null)
                        stopHandler();

                    // log worker stats
                    if (DateTime.Now - lastReportTime > TimeSpan.FromSeconds(30))
                    {
                        lastReportTime = DateTime.Now;
                        var percentWorkerTime = workerTime.Elapsed.TotalSeconds / totalTime.Elapsed.TotalSeconds;
                        logger.Debug("{0,55} work time: {1,10:##0.00%}".Format2(this.Name, percentWorkerTime));
                    }
                }
            }
            catch (Exception e)
            {
                logger.Fatal("Unhandled worker exception in {0}: ".Format2(this.Name), e);
                throw;
            }
        }
    }

    public sealed class WorkerConfig
    {
        public readonly bool initialNotify;
        public readonly TimeSpan minIdleTime;
        public readonly TimeSpan maxIdleTime;

        public WorkerConfig(bool initialNotify, TimeSpan minIdleTime, TimeSpan maxIdleTime)
        {
            this.initialNotify = initialNotify;
            this.minIdleTime = minIdleTime;
            this.maxIdleTime = maxIdleTime;
        }
    }
}
