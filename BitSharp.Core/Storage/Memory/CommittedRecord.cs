using System;

namespace BitSharp.Core.Storage.Memory
{
    internal class CommittedRecord<T>
    {
        private CommittedRecord(T value, ulong valueVersion)
        {
            Value = value;
            ValueVersion = valueVersion;
        }

        public T Value { get; private set; }

        public ulong ValueVersion { get; private set; }

        public UncommittedRecord<U> AsUncommitted<U>(Func<T, U> transformFunc)
        {
            return new UncommittedRecord<U>(transformFunc(Value), ValueVersion);
        }

        public UncommittedRecord<T> AsUncommitted()
        {
            return new UncommittedRecord<T>(Value, ValueVersion);
        }

        public bool ConflictsWith<U>(UncommittedRecord<U> uncommittedRecord)
        {
            return uncommittedRecord.ValueModified && uncommittedRecord.ValueVersion != ValueVersion;
        }

        public void Committ(UncommittedRecord<T> uncommittedRecord)
        {
            if (ConflictsWith(uncommittedRecord))
                throw new InvalidOperationException();

            if (uncommittedRecord.ValueModified)
            {
                Value = uncommittedRecord.Value;
                ValueVersion++;
            }
        }

        public void Committ<U>(UncommittedRecord<U> uncommittedRecord, Func<U, T> transformFunc)
        {
            if (uncommittedRecord.ValueModified)
            {
                Value = transformFunc(uncommittedRecord.Value);
                ValueVersion++;
            }
        }

        public static CommittedRecord<T> Initial(T value)
        {
            return new CommittedRecord<T>(value, 0);
        }
    }
}
