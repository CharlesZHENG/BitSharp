using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using BitSharp.Node.Storage;
using Ninject;
using Ninject.Modules;
using System.IO;

namespace BitSharp.Esent
{
    public class EsentStorageModule : NinjectModule
    {
        private readonly string baseDirectory;
        private readonly string[] blockTxesStorageLocations;
        private readonly string dataDirectory;
        private readonly string peersDirectory;
        private readonly ChainType chainType;
        private readonly long? cacheSizeMinBytes;
        private readonly long? cacheSizeMaxBytes;
        private readonly bool blockStorage;

        public EsentStorageModule(string baseDirectory, ChainType rulesType, bool blockStorage = true, long? cacheSizeMinBytes = null, long? cacheSizeMaxBytes = null, string[] blockTxesStorageLocations = null)
        {
            this.baseDirectory = baseDirectory;
            this.blockTxesStorageLocations = blockTxesStorageLocations;
            this.dataDirectory = Path.Combine(baseDirectory, "Data", rulesType.ToString());
            this.peersDirectory = Path.Combine(baseDirectory, "Peers", rulesType.ToString());
            this.chainType = rulesType;
            this.cacheSizeMinBytes = cacheSizeMinBytes;
            this.cacheSizeMaxBytes = cacheSizeMaxBytes;
            this.blockStorage = blockStorage;
        }

        public override void Load()
        {
            EsentStorageManager.InitSystemParameters(cacheSizeMinBytes, cacheSizeMaxBytes);

            // bind concrete storage providers
            if (blockStorage)
                this.Bind<EsentStorageManager>().ToSelf().InSingletonScope()
                    .WithConstructorArgument("baseDirectory", this.dataDirectory)
                    .WithConstructorArgument("blockTxesStorageLocations", this.blockTxesStorageLocations);

            this.Bind<NetworkPeerStorage>().ToSelf().InSingletonScope()
                .WithConstructorArgument("baseDirectory", this.peersDirectory)
                .WithConstructorArgument("chainType", this.chainType);

            // bind storage providers interfaces
            if (blockStorage)
                this.Bind<IStorageManager>().ToMethod(x => this.Kernel.Get<EsentStorageManager>()).InSingletonScope();
            this.Bind<INetworkPeerStorage>().ToMethod(x => this.Kernel.Get<NetworkPeerStorage>()).InSingletonScope();
        }
    }
}
