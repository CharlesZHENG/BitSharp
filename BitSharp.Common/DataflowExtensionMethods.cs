using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BitSharp.Common.ExtensionMethods
{
    public static class DataflowExtensionMethods
    {
        public static Task SendAndCompleteAsync<T>(this ITargetBlock<T> target, IEnumerable<T> items, CancellationToken cancelToken = default(CancellationToken))
        {
            return Task.Run(() => PostAndComplete(target, items, cancelToken));
        }

        public static void PostAndComplete<T>(this ITargetBlock<T> target, IEnumerable<T> items, CancellationToken cancelToken = default(CancellationToken))
        {
            try
            {
                foreach (var item in items)
                {
                    target.Post(item);
                    cancelToken.ThrowIfCancellationRequested();
                }

                target.Complete();
            }
            catch (Exception ex)
            {
                target.Fault(ex);
            }
        }

        public static async Task<IList<T>> ReceiveAllAsync<T>(this ISourceBlock<T> target)
        {
            var items = new List<T>();
            while (await target.OutputAvailableAsync())
                items.Add(await target.ReceiveAsync());

            await target.Completion;
            return items;
        }

        public static IEnumerable<T> ToEnumerable<T>(this ISourceBlock<T> source, CancellationToken cancelToken = default(CancellationToken))
        {
            while (source.OutputAvailableAsync(cancelToken).Result)
            {
                yield return source.Receive(cancelToken);
            }

            source.Completion.Wait();
        }
    }
}
