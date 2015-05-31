using BitSharp.Common.ExtensionMethods;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Threading;

namespace BitSharp.Common
{
    public class WorkerPool : IDisposable
    {
        private readonly Logger logger = LogManager.GetCurrentClassLogger();

        private bool isDisposed;
        private readonly string name;
        private readonly int poolThreadCount;

        private readonly WorkerMethod[] workers;

        private Action workAction;
        private int finishedCount;
        private readonly ManualResetEventSlim finishedEvent = new ManualResetEventSlim();
        private ConcurrentBag<Exception> thrownExceptions;

        public WorkerPool(string name, int poolThreadCount)
        {
            this.name = name;
            this.poolThreadCount = poolThreadCount;

            this.workers = new WorkerMethod[poolThreadCount];
            for (var i = 0; i < this.workers.Length; i++)
            {
                this.workers[i] = new WorkerMethod(name + "." + i, PerformAction, initialNotify: false, minIdleTime: TimeSpan.Zero, maxIdleTime: TimeSpan.MaxValue);
                this.workers[i].Start();
            }
        }

        public void Dispose()
        {
            if (this.isDisposed)
                return;

            this.workers.DisposeList();
            this.finishedEvent.Dispose();

            this.isDisposed = true;
        }

        public void Do(Action action)
        {
            this.Start(action);
            this.Finish();
        }

        public IDisposable Start(Action action)
        {
            if (action == null)
                throw new ArgumentNullException();
            if (this.workAction != null)
                throw new InvalidOperationException();

            this.workAction = action;
            this.finishedCount = 0;
            this.finishedEvent.Reset();
            this.thrownExceptions = new ConcurrentBag<Exception>();

            foreach (var worker in this.workers)
                worker.NotifyWork();

            return new DisposeAction(() => this.Finish());
        }

        public void Finish()
        {
            if (this.workAction == null)
                return;

            this.finishedEvent.Wait();
            this.workAction = null;

            if (this.thrownExceptions.Count > 0)
                throw new AggregateException(thrownExceptions);

        }

        private void PerformAction(WorkerMethod instance)
        {
            try
            {
                this.workAction();
            }
            catch (Exception e)
            {
                this.thrownExceptions.Add(e);
            }
            finally
            {
                if (Interlocked.Increment(ref this.finishedCount) == this.workers.Length)
                    this.finishedEvent.Set();
            }
        }
    }
}
