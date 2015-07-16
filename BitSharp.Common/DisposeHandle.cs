using System;

namespace BitSharp.Common
{
    public sealed class DisposeHandle<T> : IDisposable where T : class, IDisposable
    {
        private readonly Action<DisposeHandle<T>> disposeAction;
        private readonly T item;

        public DisposeHandle(Action<DisposeHandle<T>> disposeAction, T item)
        {
            this.disposeAction = disposeAction;
            this.item = item;
        }

        public void Dispose()
        {
            if (this.disposeAction != null)
                this.disposeAction(this);
        }

        public T Item
        {
            get { return this.item; }
        }
    }
}
