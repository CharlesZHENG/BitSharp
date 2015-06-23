using System;
using System.Collections.Generic;

namespace BitSharp.Common
{
    public interface IParallelReader<T>
    {
        bool IsStarted { get; }

        int Count { get; }

        IEnumerable<T> GetConsumingEnumerable();

        void Cancel(Exception ex);
    }
}
