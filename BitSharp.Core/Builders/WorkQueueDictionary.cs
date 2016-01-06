using NLog;
using System;
using System.Collections.Generic;
using System.Threading.Tasks.Dataflow;

namespace BitSharp.Core.Builders
{
    public class WorkQueueDictionary<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly DeferredDictionary<TKey, TValue> parentDictionary;

        private readonly BufferBlock<WorkItem> workQueue;
        private readonly Dictionary<TKey, WorkItem> workByKey;

        public WorkQueueDictionary(Func<TKey, Tuple<bool, TValue>> parentTryGetValue, Func<IEnumerable<KeyValuePair<TKey, TValue>>> parentEnumerator = null)
        {
            this.parentDictionary = new DeferredDictionary<TKey, TValue>(parentTryGetValue, parentEnumerator);
            this.workQueue = new BufferBlock<WorkItem>();
            this.workByKey = new Dictionary<TKey, WorkItem>();
        }

        public IDictionary<TKey, TValue> Updated => parentDictionary.Updated;

        public IDictionary<TKey, TValue> Added => parentDictionary.Added;

        public ISet<TKey> Deleted => parentDictionary.Deleted;

        public bool ContainsKey(TKey key)
        {
            return parentDictionary.ContainsKey(key);
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            return parentDictionary.TryGetValue(key, out value);
        }

        public bool TryAdd(TKey key, TValue value)
        {
            if (parentDictionary.TryAdd(key, value))
            {
                QueueAdd(key, value);
                return true;
            }
            else
                return false;
        }

        public bool TryRemove(TKey key)
        {
            if (parentDictionary.TryRemove(key))
            {
                QueueRemove(key);
                return true;
            }
            else
                return false;
        }

        public bool TryUpdate(TKey key, TValue value)
        {
            if (parentDictionary.TryUpdate(key, value))
            {
                QueueUpdate(key, value);
                return true;
            }
            else
                return false;
        }

        public void AddOrUpdate(TKey key, TValue value)
        {
            if (!ContainsKey(key))
            {
                if (!TryAdd(key, value))
                    throw new InvalidOperationException();
            }
            else
            {
                if (!TryUpdate(key, value))
                    throw new InvalidOperationException();
            }
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return parentDictionary.GetEnumerator();
        }

        public BufferBlock<WorkItem> WorkQueue => workQueue;

        public int WorkChangeCount { get; private set; }

        private void QueueAdd(TKey key, TValue value)
        {
            QueueWork(WorkQueueOperation.Add, key, value);
        }

        private void QueueUpdate(TKey key, TValue value)
        {
            QueueWork(WorkQueueOperation.Update, key, value);
        }

        private void QueueRemove(TKey key)
        {
            QueueWork(WorkQueueOperation.Remove, key, default(TValue));
        }

        private void QueueWork(WorkQueueOperation operation, TKey key, TValue value)
        {
            bool alreadyExists;
            WorkItem workItem;
            if (!(alreadyExists = workByKey.TryGetValue(key, out workItem)) || !workItem.TryChange(operation, value))
            {
                workItem = new WorkItem(operation, key, value);
                workByKey[key] = workItem;
                workQueue.Post(workItem);
            }
            else if (alreadyExists)
                WorkChangeCount++;
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void WarmupValue(TKey key, Func<TValue> valueFunc)
        {
            parentDictionary.WarmupValue(key, valueFunc);
        }

        public sealed class WorkItem
        {
            private WorkQueueOperation operation;
            private TKey key;
            private TValue value;
            private bool consumed;
            private readonly object lockObject = new object();

            public WorkItem(WorkQueueOperation operation, TKey key, TValue value)
            {
                this.operation = operation;
                this.key = key;
                this.value = value;
                this.consumed = false;
            }

            public bool TryChange(WorkQueueOperation newOperation, TValue newValue)
            {
                lock (lockObject)
                {
                    if (consumed)
                        return false;

                    // can't change existing add to add or existing delete to delete
                    if (newOperation == operation && operation != WorkQueueOperation.Update)
                    {
                        throw new InvalidOperationException();
                    }
                    // change existing update or add to use new value on update
                    else if (newOperation == WorkQueueOperation.Update && (operation == WorkQueueOperation.Add || operation == WorkQueueOperation.Update))
                    {
                        value = newValue;
                    }
                    // change an existing delete to an update on add
                    else if (newOperation == WorkQueueOperation.Add && operation == WorkQueueOperation.Remove)
                    {
                        operation = WorkQueueOperation.Update;
                        value = newValue;
                    }
                    // remove an existing add on delete
                    else if (newOperation == WorkQueueOperation.Remove && operation == WorkQueueOperation.Add)
                    {
                        operation = WorkQueueOperation.Nothing;
                        value = default(TValue);
                    }
                    // the remaining conditions are all invalid:
                    // - change an existing add to an update
                    // - change an existing update to a delete
                    // - change an existing delete to an update
                    else
                    {
                        throw new InvalidOperationException();
                    }

                    return true;
                }
            }

            public void Consume(Action<WorkQueueOperation, TKey, TValue> consumeAction)
            {
                lock (lockObject)
                {
                    if (consumed)
                        throw new InvalidOperationException();

                    consumed = true;
                }

                consumeAction(operation, key, value);
            }
        }
    }

    public enum WorkQueueOperation
    {
        Nothing,
        Add,
        Update,
        Remove
    }
}
