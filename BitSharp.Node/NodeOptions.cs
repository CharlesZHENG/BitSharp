using BitSharp.Core;
using BitSharp.Core.Rules;
using CommandLine;
using CommandLine.Text;
using System.Diagnostics;

namespace BitSharp.Node
{
    public class NodeOptions
    {
        [Option('d', "data-folder", DefaultValue = @"%LocalAppData%\BitSharp", HelpText = "Data folder")]
        public string DataFolder { get; set; }

        [HelpOption]
        public string GetUsage()
        {
            return HelpText.AutoBuild(this,
              (HelpText current) => HelpText.DefaultParsingErrorsHandler(this, current));
        }

        [Option("clean", DefaultValue = false, MutuallyExclusiveSet = "clean", HelpText = "Clean all data")]
        public bool Clean { get; set; }

        [Option("clean-all", DefaultValue = false, MutuallyExclusiveSet = "clean", HelpText = "Clean all files, including data and configuration")]
        public bool CleanAll { get; set; }
    }

    public class NodeConfig
    {
        public ProcessPriorityClass PriorityClass { get; set; } = ProcessPriorityClass.Normal;

        public StorageType StorageType { get; set; } = StorageType.Esent;

        public ChainType ChainType { get; set; } = ChainType.MainNet;

        public bool ConnectToPeers { get; set; } = true;

        public bool ExecuteScripts { get; set; } = true;

        public PruningMode PruningMode { get; set; } = PruningMode.TxIndex | PruningMode.BlockSpentIndex | PruningMode.BlockTxesDelete;
    }

    public class EsentConfig
    {
        public int? CacheSizeMaxMebiBytes;
    }

    public class DevConfig
    {
        public string SecondaryBlockFolder { get; set; }
    }

    public enum StorageType
    {
        Esent,
        LevelDb,
        Memory
    }
}
