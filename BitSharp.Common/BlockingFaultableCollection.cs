using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BitSharp.Common
{
    public class BlockingFaultableCollection<T> : IDisposable
    {
        private readonly BlockingCollection<T> queue = new BlockingCollection<T>();
        private readonly CancellationTokenSource cancelToken;
        private Exception fault;
        private bool disposed;

        public BlockingFaultableCollection(CancellationToken cancelToken = default(CancellationToken))
        {
            this.cancelToken = CancellationTokenSource.CreateLinkedTokenSource(cancelToken);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposed && disposing)
            {
                queue.Dispose();
                cancelToken.Dispose();
                disposed = true;
            }
        }

        public void Add(T item)
        {
            queue.Add(item);
        }

        public void CompleteAdding()
        {
            queue.CompleteAdding();
        }

        public void Fault(Exception exception)
        {
            fault = exception;
            queue.CompleteAdding();
            cancelToken.Cancel();
        }

        public IEnumerable<T> GetConsumingEnumerable(CancellationToken cancelToken = default(CancellationToken))
        {
            try
            {
                using (var linkedCancelToken = CancellationTokenSource.CreateLinkedTokenSource(cancelToken, this.cancelToken.Token))
                {
                    foreach (var item in queue.GetConsumingEnumerable(linkedCancelToken.Token))
                    {
                        if (fault != null)
                            break;

                        yield return item;
                    }
                }
            }
            finally
            {
                if (fault != null)
                    throw fault;
            }
        }
    }
}
