using BitSharp.Core.Domain;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

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
