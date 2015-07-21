using System.IO;

namespace BitSharp.Core
{
    public class StorageCorruptException : IOException
    {
        public StorageCorruptException(StorageType storageType, string message)
            : base(message)
        {
            StorageType = storageType;
        }

        public StorageType StorageType { get; private set; }
    }
}
