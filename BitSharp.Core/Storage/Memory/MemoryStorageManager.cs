using BitSharp.Common;
using BitSharp.Core.Domain;
using System.Collections.Immutable;

namespace BitSharp.Core.Storage.Memory
{
    public class MemoryStorageManager : IStorageManager
    {
        private readonly MemoryBlockStorage blockStorage;
        private readonly MemoryBlockTxesStorage blockTxesStorage;
        private readonly MemoryChainStateStorage chainStateStorage;

        public MemoryStorageManager()
            : this(null, null, null, null)
        { }

        internal MemoryStorageManager(Chain chain = null, int? unspentTxCount = null, ImmutableSortedDictionary<UInt256, UnspentTx> unspentTransactions = null, ImmutableDictionary<int, IImmutableList<SpentTx>> spentTransactions = null)
        {
            this.blockStorage = new MemoryBlockStorage();
            this.blockTxesStorage = new MemoryBlockTxesStorage();
            this.chainStateStorage = new MemoryChainStateStorage(chain, unspentTxCount, unspentTransactions, spentTransactions);
        }

        public void Dispose()
        {
            this.blockStorage.Dispose();
            this.blockTxesStorage.Dispose();
        }

        public IBlockStorage BlockStorage
        {
            get { return this.blockStorage; }
        }

        public IBlockTxesStorage BlockTxesStorage
        {
            get { return this.blockTxesStorage; }
        }

        public DisposeHandle<IChainStateCursor> OpenChainStateCursor()
        {
            return new DisposeHandle<IChainStateCursor>(null, new MemoryChainStateCursor(this.chainStateStorage));
        }
    }
}
