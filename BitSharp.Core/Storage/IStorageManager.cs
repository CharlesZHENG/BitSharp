using BitSharp.Common;
using System;

namespace BitSharp.Core.Storage
{
    public interface IStorageManager : IDisposable
    {
        IBlockStorage BlockStorage { get; }

        IBlockTxesStorage BlockTxesStorage { get; }

        DisposeHandle<IChainStateCursor> OpenChainStateCursor();
    }
}
