using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BitSharp.Core.Storage.Memory
{
    internal class UncommittedRecord<T>
    {
        private T value;

        public UncommittedRecord(T value, ulong valueVersion)
            : this(value, valueVersion, false)
        { }

        private UncommittedRecord(T value, ulong valueVersion, bool valueModified)
        {
            Value = value;
            ValueVersion = valueVersion;
            ValueModified = valueModified;
        }

        public T Value
        {
            get { return value; }
            set
            {
                this.value = value;
                ValueModified = true;
            }
        }

        public ulong ValueVersion { get; }

        public bool ValueModified { get; private set; }

        public void Modify(Action<T> modifyAction)
        {
            modifyAction(value);
            ValueModified = true;
        }

        public bool TryModify(Func<T, bool> modifyAction)
        {
            if (modifyAction(value))
            {
                ValueModified = true;
                return true;
            }
            else
                return false;
        }
    }
}
