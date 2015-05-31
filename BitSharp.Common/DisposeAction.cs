using System;

namespace BitSharp.Common
{
    public sealed class DisposeAction : IDisposable
    {
        private readonly Action disposeAction;
        private bool isDisposed;

        public DisposeAction(Action disposeAction)
        {
            this.disposeAction = disposeAction;
        }

        public void Dispose()
        {
            if (this.isDisposed)
                return;

            this.disposeAction();
            this.isDisposed = true;
        }
    }
}
