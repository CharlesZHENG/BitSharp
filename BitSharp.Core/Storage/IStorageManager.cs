using BitSharp.Common;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using System;

namespace BitSharp.Core.Storage
{
    public interface IStorageManager : IDisposable
    {
        IBlockStorage BlockStorage { get; }

        IBlockTxesStorage BlockTxesStorage { get; }

        DisposeHandle<IChainStateCursor> OpenChainStateCursor();

        DisposeHandle<IDeferredChainStateCursor> OpenDeferredChainStateCursor(IChainState chainState);

        bool IsUnconfirmedTxesConcurrent { get; }

        DisposeHandle<IUnconfirmedTxesCursor> OpenUnconfirmedTxesCursor();
    }
}
