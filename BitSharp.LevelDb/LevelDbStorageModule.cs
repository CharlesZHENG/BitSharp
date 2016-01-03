using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using BitSharp.Node.Storage;
using Ninject;
using Ninject.Modules;
using System.IO;

namespace BitSharp.LevelDb
{
    public class LevelDbStorageModule : NinjectModule
    {
        private readonly string baseDirectory;
        private readonly string[] blockTxesStorageLocations;
        private readonly string dataDirectory;
        private readonly string peersDirectory;
        private readonly ChainType chainType;
        private readonly long? cacheSizeMinBytes;
        private readonly long? cacheSizeMaxBytes;
        private readonly bool blockStorage;

        public LevelDbStorageModule(string baseDirectory, ChainType rulesType, bool blockStorage = true, long? cacheSizeMinBytes = null, long? cacheSizeMaxBytes = null, string[] blockTxesStorageLocations = null)
        {
            this.baseDirectory = baseDirectory;
            this.blockTxesStorageLocations = blockTxesStorageLocations;
            dataDirectory = Path.Combine(baseDirectory, "Data", rulesType.ToString());
            peersDirectory = Path.Combine(baseDirectory, "Peers", rulesType.ToString());
            chainType = rulesType;
            this.cacheSizeMinBytes = cacheSizeMinBytes;
            this.cacheSizeMaxBytes = cacheSizeMaxBytes;
            this.blockStorage = blockStorage;
        }

        public override void Load()
        {
            // bind concrete storage providers
            if (blockStorage)
                Bind<LevelDbStorageManager>().ToSelf().InSingletonScope()
                    .WithConstructorArgument("baseDirectory", dataDirectory)
                    .WithConstructorArgument("blockTxesStorageLocations", blockTxesStorageLocations);

            Bind<NetworkPeerStorage>().ToSelf().InSingletonScope()
                .WithConstructorArgument("baseDirectory", peersDirectory)
                .WithConstructorArgument("chainType", chainType);

            // bind storage providers interfaces
            if (blockStorage)
                Bind<IStorageManager>().ToMethod(x => Kernel.Get<LevelDbStorageManager>()).InSingletonScope();
            Bind<INetworkPeerStorage>().ToMethod(x => Kernel.Get<NetworkPeerStorage>()).InSingletonScope();
        }
    }
}
