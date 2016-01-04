using System;
using System.Threading.Tasks;

namespace BitSharp.Common.Test
{
    public class MockWorker : Worker
    {
        private readonly Func<Task> workAction;
        private readonly Action subDispose;
        private readonly Action subStart;
        private readonly Action subStop;

        public MockWorker(Func<Task> workAction = null, String name = "", bool initialNotify = false, TimeSpan? minIdleTime = null, TimeSpan? maxIdleTime = null, Action subDispose = null, Action subStart = null, Action subStop = null)
            : base(name, initialNotify, minIdleTime ?? TimeSpan.Zero, maxIdleTime ?? TimeSpan.MaxValue)
        {
            this.workAction = workAction;
            this.subDispose = subDispose;
            this.subStart = subStart;
            this.subStop = subStop;
        }

        protected override Task WorkAction()
        {
            if (this.workAction != null)
                return this.workAction();
            else
                return Task.CompletedTask;
        }

        protected override void SubDispose()
        {
            this.subDispose?.Invoke();
        }

        protected override void SubStart()
        {
            this.subStart?.Invoke();
        }

        protected override void SubStop()
        {
            this.subStop?.Invoke();
        }
    }
}
