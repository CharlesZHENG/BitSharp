using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Disposables;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Common
{
    public sealed class ResettableCancelToken : IDisposable
    {
        private readonly object lockObject = new object();

        private CancellationTokenSource cancelTokenSource = new CancellationTokenSource();
        private bool reset;

        private bool disposed;

        public void Dispose()
        {
            if (!disposed)
            {
                cancelTokenSource.Dispose();

                disposed = true;
            }
        }

        public void Cancel()
        {
            lock (lockObject)
            {
                reset = false;
                cancelTokenSource.Cancel();
            }
        }

        public void Reset()
        {
            lock (lockObject)
            {
                reset = true;
            }
        }

        public CancellationToken CancelToken()
        {
            lock (lockObject)
            {
                if (reset && cancelTokenSource.IsCancellationRequested)
                {
                    cancelTokenSource.Dispose();
                    cancelTokenSource = new CancellationTokenSource();
                }
                reset = false;

                return cancelTokenSource.Token;
            }
        }
    }
}
