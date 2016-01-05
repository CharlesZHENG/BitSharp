using BitSharp.Common;
using BitSharp.Core.Storage;
using NLog;
using System.Threading.Tasks;

namespace BitSharp.Core.Workers
{
    internal class DefragWorker : Worker
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly IStorageManager storageManager;

        public DefragWorker(WorkerConfig workerConfig, IStorageManager storageManager)
            : base("DefragWorker", workerConfig.initialNotify, workerConfig.minIdleTime, workerConfig.maxIdleTime)
        {
            this.storageManager = storageManager;
        }

        protected override Task WorkAction()
        {
            logger.Info("Begin defragging");

            this.storageManager.BlockStorage.Defragment();
            this.storageManager.BlockTxesStorage.Defragment();

            using (var handle = this.storageManager.OpenChainStateCursor())
            {
                var chainStateCursor = handle.Item;
                chainStateCursor.Defragment();
            }

            return Task.CompletedTask;
        }
    }
}
