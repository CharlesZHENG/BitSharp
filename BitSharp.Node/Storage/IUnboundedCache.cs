using System;
using System.Collections.Immutable;

namespace BitSharp.Node.Storage
{
    public interface IUnboundedCache<TKey, TValue> : IDisposable
    {
        event Action<TKey> OnMissing;

        string Name { get; }

        ImmutableHashSet<TKey> MissingData { get; }

        bool ContainsKey(TKey key);

        bool TryGetValue(TKey key, out TValue value);

        bool TryAdd(TKey key, TValue value);

        bool TryRemove(TKey key);

        TValue this[TKey key] { get; set; }

        void Flush();
    }
}
