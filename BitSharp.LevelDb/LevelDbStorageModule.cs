using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using BitSharp.Network.Storage;
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
        private readonly ulong? blocksCacheSize;
        private readonly ulong? blocksWriteCacheSize;
        private readonly ulong? blockTxesCacheSize;
        private readonly ulong? blockTxesWriteCacheSize;
        private readonly ulong? chainStateCacheSize;
        private readonly ulong? chainStateWriteCacheSize;

        public LevelDbStorageModule(string baseDirectory, ChainType rulesType,
            ulong? blocksCacheSize = null, ulong? blocksWriteCacheSize = null,
            ulong? blockTxesCacheSize = null, ulong? blockTxesWriteCacheSize = null,
            ulong? chainStateCacheSize = null, ulong? chainStateWriteCacheSize = null,
            string[] blockTxesStorageLocations = null)
        {
            this.baseDirectory = baseDirectory;
            this.blockTxesStorageLocations = blockTxesStorageLocations;
            dataDirectory = Path.Combine(baseDirectory, "Data", rulesType.ToString());
            peersDirectory = Path.Combine(baseDirectory, "Peers", rulesType.ToString());
            chainType = rulesType;

            this.blocksCacheSize = blocksCacheSize;
            this.blocksWriteCacheSize = blocksWriteCacheSize;

            this.blockTxesCacheSize = blockTxesCacheSize ?? (ulong)128.MEBIBYTE();
            this.blockTxesWriteCacheSize = blockTxesWriteCacheSize ?? (ulong)32.MEBIBYTE();

            this.chainStateCacheSize = chainStateCacheSize ?? (ulong)512.MEBIBYTE();
            this.chainStateWriteCacheSize = chainStateWriteCacheSize ?? (ulong)128.MEBIBYTE();
        }

        public override void Load()
        {
            // bind concrete storage providers
            Bind<LevelDbStorageManager>().ToSelf().InSingletonScope()
                .WithConstructorArgument("baseDirectory", dataDirectory)
                .WithConstructorArgument("blocksCacheSize", blocksCacheSize)
                .WithConstructorArgument("blocksWriteCacheSize", blocksWriteCacheSize)
                .WithConstructorArgument("blockTxesCacheSize", blockTxesCacheSize)
                .WithConstructorArgument("blockTxesWriteCacheSize", blockTxesWriteCacheSize)
                .WithConstructorArgument("chainStateCacheSize", chainStateCacheSize)
                .WithConstructorArgument("chainStateWriteCacheSize", chainStateWriteCacheSize)
                .WithConstructorArgument("blockTxesStorageLocations", blockTxesStorageLocations);

            Bind<NetworkPeerStorage>().ToSelf().InSingletonScope()
                .WithConstructorArgument("baseDirectory", peersDirectory)
                .WithConstructorArgument("chainType", chainType);

            // bind storage providers interfaces
            Bind<IStorageManager>().ToMethod(x => Kernel.Get<LevelDbStorageManager>()).InSingletonScope();
            Bind<INetworkPeerStorage>().ToMethod(x => Kernel.Get<NetworkPeerStorage>()).InSingletonScope();
        }
    }
}
