using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BitSharp.Common
{
    public static class SplitCombineBlock
    {
        public static void Create<TSplit, TCombine, TKey>(
            Func<TSplit, TKey> splitKey, Func<TCombine, TKey> combineKey,
            out TransformBlock<TSplit, TSplit> splitter, out TransformManyBlock<TCombine, TCombine> combiner,
            CancellationToken cancelToken = default(CancellationToken))
        {
            // splitter captures the original order for sorting during recombination
            var orderedKeys = new ConcurrentQueue<TKey>();
            splitter = new TransformBlock<TSplit, TSplit>(
                item =>
                {
                    orderedKeys.Enqueue(splitKey(item));
                    return item;
                },
                new ExecutionDataflowBlockOptions { CancellationToken = cancelToken });

            // recombine split items in original order
            var pendingCombined = new Dictionary<TKey, TCombine>();
            combiner = new TransformManyBlock<TCombine, TCombine>(
                item =>
                {
                    pendingCombined.Add(combineKey(item), item);
                    
                    var sortedItems = new List<TCombine>();

                    // look to see if the next item in original order has been loaded
                    // if so, dequeue and return the original item and then continue looking for the next in order
                    TKey nextKey; TCombine nextItem;
                    while (orderedKeys.TryPeek(out nextKey)
                        && pendingCombined.TryGetValue(nextKey, out nextItem))
                    {
                        sortedItems.Add(nextItem);
                        pendingCombined.Remove(nextKey);
                        orderedKeys.TryDequeue(out nextKey);
                    }

                    return sortedItems;
                },
                new ExecutionDataflowBlockOptions { CancellationToken = cancelToken });
        }
    }
}
