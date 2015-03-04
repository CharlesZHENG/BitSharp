using BitSharp.Common;
using BitSharp.Core.Storage;
using NLog;

namespace BitSharp.Esent
{
    public class EsentStorageManager : IStorageManager
    {
        private readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly string baseDirectory;

        private readonly object blockStorageLock;
        private readonly object blockTxesStorageLock;
        private readonly object chainStateManagerLock;

        private BlockStorage blockStorage;
        private BlockTxesStorage blockTxesStorage;
        private EsentChainStateManager chainStateManager;

        public EsentStorageManager(string baseDirectory)
        {
            this.baseDirectory = baseDirectory;

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
                            this.blockTxesStorage = new BlockTxesStorage(this.baseDirectory);

                return this.blockTxesStorage;
            }
        }

        public DisposeHandle<IChainStateCursor> OpenChainStateCursor()
        {
            if (this.chainStateManager == null)
                lock (this.chainStateManagerLock)
                    if (this.chainStateManager == null)
                        this.chainStateManager = new EsentChainStateManager(this.baseDirectory);

            return this.chainStateManager.OpenChainStateCursor();
        }
    }
}
