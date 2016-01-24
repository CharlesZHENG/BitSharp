using BitSharp.Common.ExtensionMethods;
using Ninject.Modules;
using NLog;
using NLog.Config;
using NLog.Targets;
using System.Diagnostics;
using System.IO;

namespace BitSharp.Node
{
    public class LoggingModule : NinjectModule
    {
        private readonly string directory;
        private readonly LogLevel logLevel;

        public LoggingModule(string baseDirectory, LogLevel logLevel)
        {
            this.directory = baseDirectory;
            this.logLevel = logLevel;
        }

        public override void Load()
        {
            // log layout format
            var layout = "${date:format=hh\\:mm\\:ss tt} ${pad:padding=6:inner=${level:uppercase=true}} ${message} ${exception:separator=\r\n:format=message,type,method,stackTrace:maxInnerExceptionLevel=10:innerExceptionSeparator=\r\n:innerFormat=message,type,method,stackTrace}";

            // initialize logging configuration
            var config = LogManager.Configuration ?? new LoggingConfiguration();

            if (!Debugger.IsAttached)
            {
                // create console target
                var consoleTarget = new ColoredConsoleTarget();
                consoleTarget.Layout = layout;
                config.AddTarget("console", consoleTarget);
                config.LoggingRules.Add(new LoggingRule("*", logLevel, consoleTarget));
            }
            else
            {
                // create debugger target
                var consoleTarget = new DebuggerTarget();
                consoleTarget.Layout = layout;
                config.AddTarget("console", consoleTarget);
                config.LoggingRules.Add(new LoggingRule("*", logLevel, consoleTarget));
            }

            // create file target
            var fileTarget = new FileTarget() { AutoFlush = false };
            fileTarget.Layout = layout;
            fileTarget.FileName = Path.Combine(this.directory, "BitSharp.log");
            fileTarget.DeleteOldFileOnStartup = true;
            config.AddTarget("file", fileTarget);
            config.LoggingRules.Add(new LoggingRule("*", logLevel, fileTarget.WrapAsync()));

            // activate configuration and bind
            LogManager.Configuration = config;
        }
    }
}
