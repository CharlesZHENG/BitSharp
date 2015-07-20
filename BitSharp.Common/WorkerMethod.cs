using System;
using System.Threading.Tasks;

namespace BitSharp.Common
{
    public class WorkerMethod : Worker
    {
        private readonly Func<WorkerMethod, Task> workAction;

        public WorkerMethod(string name, Func<WorkerMethod, Task> workAction, bool initialNotify, TimeSpan minIdleTime, TimeSpan maxIdleTime)
            : base(name, initialNotify, minIdleTime, maxIdleTime)
        {
            this.workAction = workAction;
        }

        public object Data { get; set; }

        protected override Task WorkAction()
        {
            return this.workAction(this);
        }
    }
}
