using BitSharp.Common;
using System;

namespace BitSharp.Core
{
    public class ValidationException : Exception
    {
        private readonly UInt256 blockHash;

        //[Obsolete]
        public ValidationException(UInt256 blockHash)
            : base()
        {
            this.blockHash = blockHash;
        }

        public ValidationException(UInt256 blockHash, string message)
            : base(message)
        {
            this.blockHash = blockHash;
        }

        public UInt256 BlockHash { get { return this.blockHash; } }
    }
}
