using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Common
{
    public class ConcurrentSetBuilder<T> : IEnumerable<T>, ISet<T>, ICollection<T>
    {
        private readonly ImmutableHashSet<T>.Builder builder;
        private readonly object builderLock = new object();

        public ConcurrentSetBuilder()
        {
            this.builder = ImmutableHashSet.CreateBuilder<T>();
        }

        public bool Add(T item)
        {
            lock (this.builderLock)
                return this.builder.Add(item);
        }

        public void ExceptWith(IEnumerable<T> other)
        {
            lock (this.builderLock)
                this.builder.ExceptWith(other);
        }

        public void IntersectWith(IEnumerable<T> other)
        {
            lock (this.builderLock)
                this.builder.IntersectWith(other);
        }

        public bool IsProperSubsetOf(IEnumerable<T> other)
        {
            lock (this.builderLock)
                return this.builder.IsProperSubsetOf(other);
        }

        public bool IsProperSupersetOf(IEnumerable<T> other)
        {
            lock (this.builderLock)
                return this.builder.IsProperSupersetOf(other);
        }

        public bool IsSubsetOf(IEnumerable<T> other)
        {
            lock (this.builderLock)
                return this.builder.IsSubsetOf(other);
        }

        public bool IsSupersetOf(IEnumerable<T> other)
        {
            lock (this.builderLock)
                return this.builder.IsSupersetOf(other);
        }

        public bool Overlaps(IEnumerable<T> other)
        {
            lock (this.builderLock)
                return this.builder.Overlaps(other);
        }

        public bool SetEquals(IEnumerable<T> other)
        {
            lock (this.builderLock)
                return this.builder.SetEquals(other);
        }

        public void SymmetricExceptWith(IEnumerable<T> other)
        {
            lock (this.builderLock)
                this.builder.SymmetricExceptWith(other);
        }

        public void UnionWith(IEnumerable<T> other)
        {
            lock (this.builderLock)
                this.builder.UnionWith(other);
        }

        void ICollection<T>.Add(T item)
        {
            this.Add(item);
        }

        public void Clear()
        {
            lock (this.builderLock)
                this.builder.Clear();
        }

        public bool Contains(T item)
        {
            lock (this.builderLock)
                return this.builder.Contains(item);
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            lock (this.builderLock)
                Buffer.BlockCopy(this.builder.ToArray(), 0, array, arrayIndex, this.builder.Count);
        }

        public int Count
        {
            get
            {
                lock (this.builderLock)
                    return this.builder.Count;
            }
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        public bool Remove(T item)
        {
            lock (this.builderLock)
                return this.builder.Remove(item);
        }

        public IEnumerator<T> GetEnumerator()
        {
            return this.ToImmutable().GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public ImmutableHashSet<T> ToImmutable()
        {
            lock (this.builderLock)
                return this.builder.ToImmutable();
        }
    }
}
