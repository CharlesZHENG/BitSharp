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

        internal MemoryStorageManager(Chain chain = null, int? unspentTxCount = null, int? unspentOutputCount = null, int? totalTxCount = null, int? totalInputCount = null, int? totalOutputCount = null, ImmutableSortedDictionary<UInt256, UnspentTx> unspentTransactions = null, ImmutableDictionary<int, IImmutableList<UInt256>> spentTransactions = null)
        {
            this.blockStorage = new MemoryBlockStorage();
            this.blockTxesStorage = new MemoryBlockTxesStorage();
            this.chainStateStorage = new MemoryChainStateStorage(chain, unspentTxCount, unspentOutputCount, totalTxCount, totalInputCount, totalOutputCount, unspentTransactions, spentTransactions);
        }

        public void Dispose()
        {
            this.blockStorage.Dispose();
            this.blockTxesStorage.Dispose();
            this.chainStateStorage.Dispose();
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
