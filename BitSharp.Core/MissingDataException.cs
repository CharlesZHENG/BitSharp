using System;

namespace BitSharp.Core
{
    public enum DataType
    {
        Block,
        BlockHeader,
        ChainedHeader,
        Transaction
    }

    public class MissingDataException : Exception
    {
        private readonly object key;

        public MissingDataException(object key)
        {
            this.key = key;
        }

        public object Key { get { return this.key; } }
    }
}
