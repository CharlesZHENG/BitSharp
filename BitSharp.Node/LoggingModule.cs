using BitSharp.Common.ExtensionMethods;
using Ninject.Modules;
using NLog;
using NLog.Config;
using NLog.Targets;
using NLog.Targets.Wrappers;
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
            var layout = "${pad:padding=6:inner=${level:uppercase=true}} ${message} ${exception:separator=\r\n:format=message,type,method,stackTrace:maxInnerExceptionLevel=10:innerExceptionSeparator=\r\n:innerFormat=message,type,method,stackTrace}";

            // initialize logging configuration
            var config = new LoggingConfiguration();

            // create debugger target
            var debuggerTarget = new DebuggerTarget();
            debuggerTarget.Layout = layout;
            config.AddTarget("console", debuggerTarget);
            config.LoggingRules.Add(new LoggingRule("*", logLevel, WrapAsync(debuggerTarget)));

            // create file target
            var fileTarget = new FileTarget() { AutoFlush = false };
            fileTarget.Layout = layout;
            fileTarget.FileName = Path.Combine(this.directory, "BitSharp.log");
            fileTarget.DeleteOldFileOnStartup = true;
            config.AddTarget("file", fileTarget);
            config.LoggingRules.Add(new LoggingRule("*", logLevel, WrapAsync(fileTarget)));

            // activate configuration and bind
            LogManager.Configuration = config;
        }

        private Target WrapAsync(Target target)
        {
            // don't use async logging during debugging so logging is in sync when execution is paused
            if (Debugger.IsAttached)
                return target;

            return new AsyncTargetWrapper
            {
                WrappedTarget = target,
                QueueLimit = 5.THOUSAND(),
                OverflowAction = AsyncTargetWrapperOverflowAction.Block
            };
        }
    }
}
