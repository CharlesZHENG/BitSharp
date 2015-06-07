using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Storage;
using NLog;

namespace BitSharp.Lmdb
{
    public class LmdbStorageManager : IStorageManager
    {
        private readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly string baseDirectory;
        private readonly long blockTxesSize;

        private readonly object blockStorageLock;
        private readonly object blockTxesStorageLock;
        private readonly object chainStateManagerLock;

        private BlockStorage blockStorage;
        private BlockTxesStorage blockTxesStorage;
        private LmdbChainStateManager chainStateManager;

        public LmdbStorageManager(string baseDirectory)
            : this(baseDirectory, blockTxesSize: 20.BILLION())
        {
        }

        public LmdbStorageManager(string baseDirectory, long blockTxesSize)
        {
            this.baseDirectory = baseDirectory;
            this.blockTxesSize = blockTxesSize;

            this.blockStorageLock = new object();
            this.blockTxesStorageLock = new object();
            this.chainStateManagerLock = new object();
        }

        public void Dispose()
        {
            if (this.chainStateManager != null)
                this.chainStateManager.Dispose();

            if (this.blockStorage != null)
                this.blockStorage.Dispose();

            if (this.blockTxesStorage != null)
                this.blockTxesStorage.Dispose();
        }

        public IBlockStorage BlockStorage
        {
            get
            {
                if (this.blockStorage == null)
                    lock (this.blockStorageLock)
                        if (this.blockStorage == null)
                            this.blockStorage = new BlockStorage(this.baseDirectory);

                return this.blockStorage;
            }
        }

        public IBlockTxesStorage BlockTxesStorage
        {
            get
            {
                if (this.blockTxesStorage == null)
                    lock (this.blockTxesStorageLock)
                        if (this.blockTxesStorage == null)
                            this.blockTxesStorage = new BlockTxesStorage(this.baseDirectory, this.blockTxesSize);

                return this.blockTxesStorage;
            }
        }

        public DisposeHandle<IChainStateCursor> OpenChainStateCursor()
        {
            if (this.chainStateManager == null)
                lock (this.chainStateManagerLock)
                    if (this.chainStateManager == null)
                        this.chainStateManager = new LmdbChainStateManager(this.baseDirectory);

            return this.chainStateManager.OpenChainStateCursor();
        }
    }
}
