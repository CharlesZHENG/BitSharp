using System;
using System.Collections.Generic;

namespace BitSharp.Node.Storage
{
    public interface IBoundedCache<TKey, TValue> : IUnboundedCache<TKey, TValue>
    {
        event Action<TKey, TValue> OnAddition;
        event Action<TKey, TValue> OnModification;
        event Action<TKey> OnRemoved;

        int Count { get; }

        IEnumerable<TKey> Keys { get; }

        IEnumerable<TValue> Values { get; }
    }
}
