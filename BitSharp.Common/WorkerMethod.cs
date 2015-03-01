using NLog;
using System;

namespace BitSharp.Common
{
    public class WorkerMethod : Worker
    {
        private readonly Action<WorkerMethod> workAction;

        public WorkerMethod(string name, Action<WorkerMethod> workAction, bool initialNotify, TimeSpan minIdleTime, TimeSpan maxIdleTime, Logger logger)
            : base(name, initialNotify, minIdleTime, maxIdleTime, logger)
        {
            this.workAction = workAction;
        }

        public object Data { get; set; }

        protected override void WorkAction()
        {
            this.workAction(this);
        }
    }
}
