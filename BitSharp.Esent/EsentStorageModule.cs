using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using BitSharp.Node.Storage;
using Microsoft.Isam.Esent.Collections.Generic;
using Microsoft.Isam.Esent.Interop;
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
        private readonly RulesEnum rulesType;
        private readonly long? cacheSizeMinBytes;
        private readonly long? cacheSizeMaxBytes;
        private readonly bool blockStorage;

        public EsentStorageModule(string baseDirectory, RulesEnum rulesType, bool blockStorage = true, long? cacheSizeMinBytes = null, long? cacheSizeMaxBytes = null, string[] blockTxesStorageLocations = null)
        {
            this.baseDirectory = baseDirectory;
            this.blockTxesStorageLocations = blockTxesStorageLocations;
            this.dataDirectory = Path.Combine(baseDirectory, "Data", rulesType.ToString());
            this.peersDirectory = Path.Combine(baseDirectory, "Peers", rulesType.ToString());
            this.rulesType = rulesType;
            this.cacheSizeMinBytes = cacheSizeMinBytes;
            this.cacheSizeMaxBytes = cacheSizeMaxBytes;
            this.blockStorage = blockStorage;
        }

        public override void Load()
        {
            //TODO remove reflection once PersistentDictionary is phased out
            var esentAssembly = typeof(PersistentDictionary<string, string>).Assembly;
            var type = esentAssembly.GetType("Microsoft.Isam.Esent.Collections.Generic.CollectionsSystemParameters");
            var method = type.GetMethod("Init");
            method.Invoke(null, null);
            if (this.cacheSizeMinBytes != null)
                SystemParameters.CacheSizeMin = (this.cacheSizeMinBytes.Value / SystemParameters.DatabasePageSize).ToIntChecked();
            if (this.cacheSizeMaxBytes != null)
                SystemParameters.CacheSizeMax = (this.cacheSizeMaxBytes.Value / SystemParameters.DatabasePageSize).ToIntChecked();

            // bind concrete storage providers
            if (blockStorage)
                this.Bind<EsentStorageManager>().ToSelf().InSingletonScope()
                    .WithConstructorArgument("baseDirectory", this.dataDirectory)
                    .WithConstructorArgument("blockTxesStorageLocations", this.blockTxesStorageLocations);

            this.Bind<NetworkPeerStorage>().ToSelf().InSingletonScope()
                .WithConstructorArgument("baseDirectory", this.peersDirectory)
                .WithConstructorArgument("rulesType", this.rulesType);

            // bind storage providers interfaces
            if (blockStorage)
                this.Bind<IStorageManager>().ToMethod(x => this.Kernel.Get<EsentStorageManager>()).InSingletonScope();
            this.Bind<INetworkPeerStorage>().ToMethod(x => this.Kernel.Get<NetworkPeerStorage>()).InSingletonScope();
        }
    }
}
