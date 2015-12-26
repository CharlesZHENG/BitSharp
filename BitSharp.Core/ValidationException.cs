using BitSharp.Common;
using System;

namespace BitSharp.Core
{
    public class ValidationException : Exception
    {
        //[Obsolete]
        public ValidationException(UInt256 blockHash)
            : base($"Invalid block: {blockHash}")
        {
            BlockHash = blockHash;
        }

        public ValidationException(UInt256 blockHash, string message)
            : base(message)
        {
            BlockHash = blockHash;
        }

        public UInt256 BlockHash { get; }
    }
}
