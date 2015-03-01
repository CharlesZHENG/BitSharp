using BitSharp.Common;
using BitSharp.Core.Storage;
using NLog;

namespace BitSharp.Core.Workers
{
    internal class DefragWorker : Worker
    {
        private readonly Logger logger;
        private readonly IStorageManager storageManager;

        public DefragWorker(WorkerConfig workerConfig, IStorageManager storageManager, Logger logger)
            : base("DefragWorker", workerConfig.initialNotify, workerConfig.minIdleTime, workerConfig.maxIdleTime, logger)
        {
            this.logger = logger;
            this.storageManager = storageManager;
        }

        protected override void WorkAction()
        {
            this.logger.Info("Begin defragging");

            this.storageManager.BlockStorage.Defragment();
            this.storageManager.BlockTxesStorage.Defragment();

            using (var handle = this.storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;
                chainStateCursor.Defragment();
            }
        }
    }
}
