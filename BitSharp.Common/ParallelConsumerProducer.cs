using BitSharp.Common.ExtensionMethods;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Common
{
    public class ParallelConsumerProducer<TIn, TOut> : IParallelReader<TOut>, IDisposable
    {
        private bool isDisposed;

        private readonly ReaderWriterLockSlim controlLock = new ReaderWriterLockSlim();
        private readonly ParallelObserver<TIn> consumer;
        private readonly ParallelReader<TOut> producer;

        private TaskCompletionSource<object> tcs;
        private ParallelReader<TIn> source;
        private IObserver<TIn> observer;
        private CancellationTokenSource cancelToken;
        private CancellationTokenSource internalCancelToken;
        private ConcurrentBlockingQueue<TOut> producerQueue;
        private Task consumerTask;
        private Task producerTask;

        public ParallelConsumerProducer(string name, int consumerThreadCount)
        {
            this.consumer = new ParallelObserver<TIn>("{0}.Consumer".Format2(name), consumerThreadCount);
            this.producer = new ParallelReader<TOut>("{0}.Producer".Format2(name));
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!isDisposed && disposing)
            {
                CleanupPendingConsumeProduce();
                this.consumer.Dispose();
                this.producer.Dispose();

                isDisposed = true;
            }
        }

        public Task ConsumeProduceAsync(ParallelReader<TIn> source, IObserver<TIn> observer, Func<TIn, IEnumerable<TOut>> producerFunc, CancellationToken? cancelToken = null)
        {
            controlLock.EnterWriteLock();
            try
            {
                if (tcs != null)
                    throw new InvalidOperationException();

                tcs = new TaskCompletionSource<object>();
                this.source = source;
                this.observer = observer;
                if (cancelToken.HasValue)
                {
                    internalCancelToken = new CancellationTokenSource();
                    this.cancelToken = CancellationTokenSource.CreateLinkedTokenSource(internalCancelToken.Token, cancelToken.Value);
                }
                else
                {
                    internalCancelToken = null;
                    this.cancelToken = new CancellationTokenSource();
                }

                // begin the producer parallel reader to make available any produced results
                producerQueue = new ConcurrentBlockingQueue<TOut>();
                producerTask = producer.ReadAsync(producerQueue.GetConsumingEnumerable(), this.cancelToken.Token);

                // begin the consumer+producer task
                consumerTask = consumer.SubscribeObservers(source,
                    Observer.Create<TIn>(
                        inItem =>
                        {
                            // consume an input item
                            observer.OnNext(inItem);
                            this.cancelToken.Token.ThrowIfCancellationRequested();

                            // produce output items
                            foreach (var outItem in producerFunc(inItem))
                            {
                                producerQueue.Add(outItem);
                                this.cancelToken.Token.ThrowIfCancellationRequested();
                            }
                        },
                        ex => Finish(producerQueue, ex),
                        () => Finish(producerQueue)));

                return tcs.Task;
            }
            finally
            {
                controlLock.ExitWriteLock();
            }
        }

        public IEnumerable<TOut> GetConsumingEnumerable()
        {
            controlLock.EnterReadLock();
            try
            {
                foreach (var outItem in this.producer.GetConsumingEnumerable())
                    yield return outItem;
            }
            finally
            {
                controlLock.ExitReadLock();
            }
        }

        private void Finish(ConcurrentBlockingQueue<TOut> producerQueue, Exception ex = null)
        {
            controlLock.EnterUpgradeableReadLock();
            try
            {
                producerQueue.CompleteAdding();

                // cancel readers on exception
                if (ex != null)
                {
                    source.Cancel();
                    producer.Cancel();
                }

                // wait for the producer task to finish, capture any exception
                try
                {
                    producerTask.Wait();
                }
                catch (Exception taskEx)
                {
                    ex = ex ?? taskEx;
                }

                // producer is finished, cleanup and mark task completed
                controlLock.EnterWriteLock();
                try
                {
                    producerQueue.Dispose();

                    try
                    {
                        if (ex != null)
                            observer.OnError(ex);
                        else
                            observer.OnCompleted();
                    }
                    catch (Exception) { }

                    if (ex != null)
                        tcs.SetException(ex);
                    else
                        tcs.SetResult(null);

                    tcs = null;
                }
                finally
                {
                    controlLock.ExitWriteLock();
                }
            }
            finally
            {
                controlLock.ExitUpgradeableReadLock();
            }
        }

        bool IParallelReader<TOut>.IsStarted
        {
            get { return producer.IsStarted; }
        }

        int IParallelReader<TOut>.Count
        {
            get { return producer.Count; }
        }

        void IParallelReader<TOut>.Cancel()
        {
            producer.Cancel();
        }

        // attempt to cleanup any outstanding consume+produce during disposal, without causing Dispose() to throw errors
        private void CleanupPendingConsumeProduce()
        {
            try
            {
                if (cancelToken != null)
                {
                    cancelToken.Cancel();
                    cancelToken.Dispose();
                }
            }
            catch (Exception) { }

            try
            {
                if (internalCancelToken != null)
                {
                    internalCancelToken.Cancel();
                    internalCancelToken.Dispose();
                }
            }
            catch (Exception) { }
        }
    }
}
