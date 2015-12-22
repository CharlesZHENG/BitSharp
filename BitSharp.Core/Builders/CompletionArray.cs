using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;

namespace BitSharp.Core.Builders
{
    public class CompletionArray<T>
    {
        private readonly ImmutableArray<T>.Builder array;
        private ImmutableArray<T>? completedArray;
        private bool[] completed;

        public CompletionArray(int length)
        {
            this.array = ImmutableArray.CreateBuilder<T>(length);
            for (var i = 0; i < length; i++)
                this.array.Add(default(T));

            this.completed = new bool[length];

            if (length == 0)
                this.completedArray = this.array.MoveToImmutable();
        }

        public ImmutableArray<T> CompletedArray
        {
            get
            {
                if (this.completedArray == null)
                    throw new InvalidOperationException();

                return this.completedArray.Value;
            }
        }

        public bool IsComplete => completedArray != null;

        public bool TryComplete(int index, T value)
        {
            if (value == null)
                throw new ArgumentNullException(nameof(value));

            lock (this.array)
            {
                if (this.completedArray != null || this.completed[index])
                    throw new InvalidOperationException();

                this.array[index] = value;
                this.completed[index] = true;

                var completed = this.completed.All(x => x);
                if (completed)
                    this.completedArray = this.array.MoveToImmutable();

                return completed;
            }
        }
    }

    public class CompletionCount
    {
        private int count;

        public CompletionCount(int count)
        {
            this.count = count;
        }

        public bool TryComplete()
        {
            return Interlocked.Decrement(ref count) == 0;
        }

        public bool IsComplete => count == 0;
    }
}
