using System;

namespace BitSharp.Common
{
    public sealed class DisposeHandle<T> : IDisposable where T : class, IDisposable
    {
        private readonly Action<DisposeHandle<T>> disposeAction;
        private readonly T item;

        private bool disposed;

        public DisposeHandle(Action<DisposeHandle<T>> disposeAction, T item)
        {
            this.disposeAction = disposeAction;
            this.item = item;
        }

        public void Dispose()
        {
            if (!disposed)
            {
                this.disposeAction?.Invoke(this);

                disposed = true;
            }
        }

        public T Item
        {
            get { return this.item; }
        }
    }
}
