using BitSharp.Common;
using System;

namespace BitSharp.Node.Domain
{
    public class InventoryVector
    {
        public const UInt32 TYPE_ERROR = 0;
        public const UInt32 TYPE_MESSAGE_TRANSACTION = 1;
        public const UInt32 TYPE_MESSAGE_BLOCK = 2;

        public readonly UInt32 Type;
        public readonly UInt256 Hash;

        public InventoryVector(UInt32 Type, UInt256 Hash)
        {
            this.Type = Type;
            this.Hash = Hash;
        }
    }
}
