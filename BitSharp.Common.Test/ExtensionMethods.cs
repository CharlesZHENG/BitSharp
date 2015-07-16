using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks.Dataflow;
using System.Reactive.Linq;

namespace BitSharp.Common.Test
{
    public static class ExtensionMethods
    {
        public static IEnumerable<T> ToEnumerable<T>(this ISourceBlock<T> source, CancellationToken cancelToken = default(CancellationToken))
        {
            return source.AsObservable().ToEnumerable();
        }

        public static BufferBlock<T> ToBufferBlock<T>(this IEnumerable<T> items)
        {
            var bufferBlock = new BufferBlock<T>();

            foreach (var item in items)
                bufferBlock.Post(item);
            bufferBlock.Complete();

            return bufferBlock;
        }
    }
}
