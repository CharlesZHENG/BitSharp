﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BitSharp.Storage
{
    public interface IUnboundedStorage<TKey, TValue> : IDisposable
    {
        bool ContainsKey(TKey key);

        bool TryGetValue(TKey key, out TValue value);

        bool TryAdd(TKey key, TValue value);

        bool TryRemove(TKey key);

        TValue this[TKey key] { get; set; }

        void Flush();
    }
}
