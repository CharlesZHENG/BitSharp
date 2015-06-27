using BitSharp.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BitSharp.Core.Builders
{
    internal static class BuilderExtensionMethods
    {
        public static Task SendAndCompleteAsync<T>(this ITargetBlock<T> target, IEnumerable<T> items)
        {
            return Task.Run(() =>
            {
                try
                {
                    foreach (var item in items)
                        target.Post(item);

                    target.Complete();
                }
                catch (Exception ex)
                {
                    target.Fault(ex);
                }
            });
        }

        public static async Task<IList<T>> ReceiveAllAsync<T>(this ISourceBlock<T> target)
        {
            var items = new List<T>();
            while (await target.OutputAvailableAsync())
                items.Add(await target.ReceiveAsync());

            await target.Completion;
            return items;
        }

        public static ConcurrentBlockingQueue<T> LinkToQueue<T>(this ISourceBlock<T> source)
        {
            var queue = new ConcurrentBlockingQueue<T>();

            var queueItems = new ActionBlock<T>(item => queue.Add(item));
            queueItems.Completion.ContinueWith(_ => queue.CompleteAdding());

            source.LinkTo(queueItems, new DataflowLinkOptions { PropagateCompletion = true });

            return queue;
        }
    }
}
