using CommandLine;
using CommandLine.Text;

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
    }
}
