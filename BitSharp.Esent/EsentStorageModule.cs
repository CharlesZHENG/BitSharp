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

        public EsentStorageModule(string baseDirectory, ChainType rulesType, long? cacheSizeMinBytes = null, long? cacheSizeMaxBytes = null, string[] blockTxesStorageLocations = null)
        {
            this.baseDirectory = baseDirectory;
            this.blockTxesStorageLocations = blockTxesStorageLocations;
            this.dataDirectory = Path.Combine(baseDirectory, "Data", rulesType.ToString());
            this.peersDirectory = Path.Combine(baseDirectory, "Peers", rulesType.ToString());
            this.chainType = rulesType;
            this.cacheSizeMinBytes = cacheSizeMinBytes;
            this.cacheSizeMaxBytes = cacheSizeMaxBytes;
        }

        public override void Load()
        {
            EsentStorageManager.InitSystemParameters(cacheSizeMinBytes, cacheSizeMaxBytes);

            // bind concrete storage providers
            this.Bind<EsentStorageManager>().ToSelf().InSingletonScope()
                .WithConstructorArgument("baseDirectory", this.dataDirectory)
                .WithConstructorArgument("blockTxesStorageLocations", this.blockTxesStorageLocations);

            this.Bind<NetworkPeerStorage>().ToSelf().InSingletonScope()
                .WithConstructorArgument("baseDirectory", this.peersDirectory)
                .WithConstructorArgument("chainType", this.chainType);

            // bind storage providers interfaces
            this.Bind<IStorageManager>().ToMethod(x => this.Kernel.Get<EsentStorageManager>()).InSingletonScope();
            this.Bind<INetworkPeerStorage>().ToMethod(x => this.Kernel.Get<NetworkPeerStorage>()).InSingletonScope();
        }
    }
}
