using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BitSharp.Common
{
    public static class OnCompleteBlock
    {
        /// <summary>
        /// Create a block which passes through items from a source block, and calls an action before completing.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="onCompleteAction"></param>
        /// <param name="cancelToken"></param>
        /// <returns></returns>
        public static ISourceBlock<T> Create<T>(ISourceBlock<T> source, Action onCompleteAction, CancellationToken cancelToken = default(CancellationToken))
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));
            if (onCompleteAction == null)
                throw new ArgumentNullException(nameof(onCompleteAction));

            var passthrough = new TransformBlock<T, T>(item => item);
            source.LinkTo(passthrough, new DataflowLinkOptions { PropagateCompletion = false });

            source.Completion.ContinueWith(
                task =>
                {
                    var passthroughBlock = (IDataflowBlock)passthrough;
                    try
                    {
                        if (task.Status == TaskStatus.RanToCompletion)
                            onCompleteAction();

                        if (task.IsCanceled)
                            passthroughBlock.Fault(new OperationCanceledException());
                        else if (task.IsFaulted)
                            passthroughBlock.Fault(task.Exception);
                        else
                            passthrough.Complete();
                    }
                    catch (Exception ex)
                    {
                        passthroughBlock.Fault(ex);
                    }
                }, cancelToken);

            return passthrough;
        }
    }
}
