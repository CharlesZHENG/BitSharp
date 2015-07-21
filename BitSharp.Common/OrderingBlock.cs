using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BitSharp.Common
{
    public static class OrderingBlock
    {
        public static OrderedSource<TInput, TOutput, TKey> CaptureOrder<TInput, TOutput, TKey>(
            ISourceBlock<TInput> source, Func<TInput, TKey> keyFunc,
            CancellationToken cancelToken = default(CancellationToken))
        {
            // capture the original order to be reapplied later
            var orderedKeys = new ConcurrentQueue<TKey>();
            var orderCapturer = new TransformBlock<TInput, TInput>(
                item =>
                {
                    orderedKeys.Enqueue(keyFunc(item));
                    return item;
                },
                new ExecutionDataflowBlockOptions { CancellationToken = cancelToken });

            source.LinkTo(orderCapturer, new DataflowLinkOptions { PropagateCompletion = true });

            return new OrderedSource<TInput, TOutput, TKey>(orderedKeys, orderCapturer);
        }

        public class OrderedSource<TSplit, TCombine, TKey> : ISourceBlock<TSplit>
        {
            private readonly ConcurrentQueue<TKey> orderedKeys;
            private readonly ISourceBlock<TSplit> orderCapturer;
            private bool sorted;

            internal OrderedSource(ConcurrentQueue<TKey> orderedKeys, ISourceBlock<TSplit> orderCapturer)
            {
                this.orderedKeys = orderedKeys;
                this.orderCapturer = orderCapturer;
            }

            public ISourceBlock<TCombine> ApplyOrder(ISourceBlock<TCombine> source, Func<TCombine, TKey> keyFunc, CancellationToken cancelToken = default(CancellationToken))
            {
                if (sorted)
                    throw new InvalidOperationException($"{GetType().Name} has already been sorted.");

                // sort items back into original order
                var pendingItems = new Dictionary<TKey, TCombine>();
                var sorter = new TransformManyBlock<TCombine, TCombine>(
                    item =>
                    {
                        pendingItems.Add(keyFunc(item), item);

                        var sortedItems = new List<TCombine>();

                        // look to see if the next item in original order has been loaded
                        // if so, dequeue and return the original item and then continue looking for the next in order
                        TKey nextKey; TCombine nextItem;
                        while (orderedKeys.TryPeek(out nextKey)
                            && pendingItems.TryGetValue(nextKey, out nextItem))
                        {
                            sortedItems.Add(nextItem);
                            pendingItems.Remove(nextKey);
                            orderedKeys.TryDequeue(out nextKey);
                        }

                        return sortedItems;
                    },
                    new ExecutionDataflowBlockOptions { CancellationToken = cancelToken });

                source.LinkTo(sorter, new DataflowLinkOptions { PropagateCompletion = true });

                sorted = true;
                return sorter;
            }

            public void Complete()
            {
                orderCapturer.Complete();
            }

            public Task Completion
            {
                get { return orderCapturer.Completion; }
            }

            public IDisposable LinkTo(ITargetBlock<TSplit> target, DataflowLinkOptions linkOptions)
            {
                return orderCapturer.LinkTo(target, linkOptions);
            }

            TSplit ISourceBlock<TSplit>.ConsumeMessage(DataflowMessageHeader messageHeader, ITargetBlock<TSplit> target, out bool messageConsumed)
            {
                return orderCapturer.ConsumeMessage(messageHeader, target, out messageConsumed);
            }

            void ISourceBlock<TSplit>.ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<TSplit> target)
            {
                orderCapturer.ReleaseReservation(messageHeader, target);
            }

            bool ISourceBlock<TSplit>.ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<TSplit> target)
            {
                return orderCapturer.ReserveMessage(messageHeader, target);
            }

            void IDataflowBlock.Fault(Exception exception)
            {
                orderCapturer.Fault(exception);
            }
        }
    }
}
