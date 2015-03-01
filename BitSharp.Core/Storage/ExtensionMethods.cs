using System;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Core.Storage
{
    public static class StorageExtensionMethods
    {
        public static ImmutableDictionary<TKey, TValue> Compact<TKey, TValue>(this ImmutableDictionary<TKey, TValue> dictionary)
        {
            return ImmutableDictionary.CreateRange(dictionary.ToArray());
        }

        public static ImmutableHashSet<T> Compact<T>(this ImmutableHashSet<T> set)
        {
            return ImmutableHashSet.Create<T>(set.ToArray());
        }

        public static ImmutableList<T> Compact<T>(this ImmutableList<T> list)
        {
            return ImmutableList.Create<T>(list.ToArray());
        }

        public static bool IsMissingDataOnly(this AggregateException e)
        {
            return e.InnerExceptions.All(x => x is MissingDataException);
        }
    }
}
