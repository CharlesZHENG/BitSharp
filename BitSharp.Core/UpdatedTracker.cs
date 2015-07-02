using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Disposables;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Core
{
    public sealed class UpdatedTracker : IDisposable
    {
        private readonly ManualResetEventSlim updatedEvent = new ManualResetEventSlim();
        private readonly object changedLock = new object();
        private long version;

        public void Dispose()
        {
            updatedEvent.Dispose();
        }

        public void WaitForUpdate()
        {
            updatedEvent.Wait();
        }

        public void WaitForUpdate(CancellationToken cancellationToken)
        {
            updatedEvent.Wait(cancellationToken);
        }

        public bool WaitForUpdate(TimeSpan timeout)
        {
            return updatedEvent.Wait(timeout);
        }

        public bool WaitForUpdate(TimeSpan timeout, CancellationToken cancellationToken)
        {
            return updatedEvent.Wait(timeout, cancellationToken);
        }

        public IDisposable TryUpdate(Action staleAction)
        {
            long origVersion;
            lock (this.changedLock)
                origVersion = this.version;

            return Disposable.Create(() =>
            {
                lock (this.changedLock)
                {
                    if (this.version == origVersion)
                        this.updatedEvent.Set();
                    else
                        staleAction();
                }
            });
        }

        public void MarkStale()
        {
            lock (this.changedLock)
            {
                this.version++;
                this.updatedEvent.Reset();
            }
        }
    }
}
