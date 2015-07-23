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
        public MissingDataException(object key)
        {
            Key = key;
        }

        public object Key { get; }
    }
}
