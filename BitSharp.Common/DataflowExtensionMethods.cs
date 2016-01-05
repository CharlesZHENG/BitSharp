using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BitSharp.Common.ExtensionMethods
{
    public static class DataflowExtensionMethods
    {
        public static async Task SendAndCompleteAsync<T>(this ITargetBlock<T> target, IEnumerable<T> items, CancellationToken cancelToken = default(CancellationToken))
        {
            await Task.Yield();
            try
            {
                foreach (var item in items)
                {
                    if (target.Completion.IsFaulted)
                        throw target.Completion.Exception;

                    if (!await target.SendAsync(item, cancelToken))
                        throw new InvalidOperationException();

                    cancelToken.ThrowIfCancellationRequested();
                }

                target.Complete();
            }
            catch (Exception ex)
            {
                target.Fault(ex);
                throw;
            }
        }

        public static void PostAndComplete<T>(this ITargetBlock<T> target, IEnumerable<T> items, CancellationToken cancelToken = default(CancellationToken))
        {
            try
            {
                foreach (var item in items)
                {
                    if (!target.Post(item))
                        throw new InvalidOperationException();

                    cancelToken.ThrowIfCancellationRequested();
                }

                target.Complete();
            }
            catch (Exception ex)
            {
                target.Fault(ex);
                throw;
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
    }
}
