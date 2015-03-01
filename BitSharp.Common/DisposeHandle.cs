using System;

namespace BitSharp.Common
{
    public sealed class DisposeHandle<T> : IDisposable where T : class, IDisposable
    {
        private readonly Action disposeAction;
        private readonly T item;

        private bool isDisposed;

        public DisposeHandle(Action disposeAction, T item)
        {
            this.disposeAction = disposeAction;
            this.item = item;
        }

        ~DisposeHandle()
        {
            this.Dispose();
        }

        public void Dispose()
        {
            if (this.isDisposed)
                return;

            if (this.disposeAction != null)
                this.disposeAction();

            this.isDisposed = true;
            GC.SuppressFinalize(this);
        }

        public T Item
        {
            get { return this.item; }
        }
    }
}
