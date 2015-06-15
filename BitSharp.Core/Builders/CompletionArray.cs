using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BitSharp.Core.Builders
{
    public class CompletionArray<T> where T : class
    {
        private readonly ImmutableArray<T>.Builder array;
        private ImmutableArray<T> completedArray;

        public CompletionArray(int length)
        {
            this.array = ImmutableArray.CreateBuilder<T>(length);
            for (var i = 0; i < length; i++)
                this.array.Add(null);
        }

        public ImmutableArray<T> CompletedArray
        {
            get
            {
                if (this.completedArray == null)
                    throw new InvalidOperationException();

                return this.completedArray;
            }
        }

        public bool TryComplete(int index, T value)
        {
            lock (this.array)
            {
                if (this.completedArray != null || this.array[index] != null)
                    throw new InvalidOperationException();

                this.array[index] = value;

                var completed = this.array.All(x => x != null);
                if (completed)
                    this.completedArray = this.array.MoveToImmutable();

                return completed;
            }
        }
    }
}
