using BitSharp.Common;
using BitSharp.Core.Domain;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BitSharp.Core.Storage
{
    public interface ICoreStorage : IDisposable
    {
        bool TryGetTransaction(UInt256 blockHash, int txIndex, out Transaction transaction);
    }
}
