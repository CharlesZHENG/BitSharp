using BitSharp.Common.ExtensionMethods;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Threading;

namespace BitSharp.Common
{
    public class WorkerPool : IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private bool isDisposed;
        private readonly string name;
        private readonly int poolThreadCount;

        private readonly WorkerMethod[] workers;
        private readonly CountdownEvent finishedEvent;

        private Action<int> workAction;
        private ConcurrentBag<Exception> thrownExceptions;

        public WorkerPool(string name, int poolThreadCount)
        {
            this.name = name;
            this.poolThreadCount = poolThreadCount;

            this.finishedEvent = new CountdownEvent(poolThreadCount);
            this.workers = new WorkerMethod[poolThreadCount];

            for (var i = 0; i < this.workers.Length; i++)
            {
                this.workers[i] = new WorkerMethod(name + "." + i, PerformAction, initialNotify: false, minIdleTime: TimeSpan.Zero, maxIdleTime: TimeSpan.MaxValue)
                {
                    Data = i
                };
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

        public void Do(Action<int> action)
        {
            this.Start(action);
            this.Finish();
        }

        public void Do(Action action)
        {
            this.Start(action);
            this.Finish();
        }

        public IDisposable Start(Action action)
        {
            return Start(_ => action());
        }

        public IDisposable Start(Action<int> action)
        {
            if (action == null)
                throw new ArgumentNullException();
            if (this.workAction != null)
                throw new InvalidOperationException();

            this.workAction = action;
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
                this.workAction((int)instance.Data);
            }
            catch (Exception e)
            {
                this.thrownExceptions.Add(e);
            }
            finally
            {
                finishedEvent.Signal();
            }
        }
    }
}
