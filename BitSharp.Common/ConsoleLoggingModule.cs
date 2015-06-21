using Ninject.Modules;
using NLog;
using NLog.Config;
using NLog.Targets;
using System.Diagnostics;

namespace BitSharp.Common
{
    public class ConsoleLoggingModule : NinjectModule
    {
        private readonly LogLevel logLevel;

        public ConsoleLoggingModule(LogLevel logLevel = null)
        {
            this.logLevel = logLevel ?? LogLevel.Debug;
        }

        public override void Load()
        {
            // log layout format
            var layout = "${date:format=hh\\:mm\\:ss tt} ${pad:padding=6:inner=${level:uppercase=true}} ${message} ${exception:separator=\r\n:format=message,type,method,stackTrace:maxInnerExceptionLevel=10:innerExceptionSeparator=\r\n:innerFormat=message,type,method,stackTrace}";

            // initialize logging configuration
            var config = new LoggingConfiguration();

            // create console target
            if (!Debugger.IsAttached)
            {
                var consoleTarget = new ColoredConsoleTarget();
                consoleTarget.Layout = layout;
                config.AddTarget("console", consoleTarget);
                config.LoggingRules.Add(new LoggingRule("*", this.logLevel, consoleTarget));
            }
            else
            {
                var consoleTarget = new DebuggerTarget();
                consoleTarget.Layout = layout;
                config.AddTarget("console", consoleTarget);
                config.LoggingRules.Add(new LoggingRule("*", this.logLevel, consoleTarget));
            }

            // activate configuration and bind
            LogManager.Configuration = config;
        }
    }
}
