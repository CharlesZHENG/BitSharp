using BitSharp.Common.ExtensionMethods;
using NLog;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace BitSharp.Common
{
    public class MethodTimer
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        public MethodTimer()
        {
            this.IsEnabled = true;
        }

        public MethodTimer(bool isEnabled)
        {
            this.IsEnabled = isEnabled;
        }

        public bool IsEnabled { get; set; }

        [DebuggerStepThrough]
        public void Time(Action action, [CallerMemberName] string memberName = "", [CallerLineNumber] int lineNumber = 0)
        {
            Time(action, null, -1, memberName, lineNumber);
        }

        [DebuggerStepThrough]
        public void Time(string timerName, Action action, [CallerMemberName] string memberName = "", [CallerLineNumber] int lineNumber = 0)
        {
            Time(action, timerName, -1, memberName, lineNumber);
        }

        [DebuggerStepThrough]
        public void Time(long filterTime, Action action, [CallerMemberName] string memberName = "", [CallerLineNumber] int lineNumber = 0)
        {
            Time(action, null, filterTime, memberName, lineNumber);
        }

        [DebuggerStepThrough]
        public void Time(string timerName, long filterTime, Action action, [CallerMemberName] string memberName = "", [CallerLineNumber] int lineNumber = 0)
        {
            Time(action, timerName, filterTime, memberName, lineNumber);
        }

        [DebuggerStepThrough]
        public T Time<T>(Func<T> func, [CallerMemberName] string memberName = "", [CallerLineNumber] int lineNumber = 0)
        {
            return Time(func, null, -1, memberName, lineNumber);
        }

        [DebuggerStepThrough]
        public T Time<T>(string timerName, Func<T> func, [CallerMemberName] string memberName = "", [CallerLineNumber] int lineNumber = 0)
        {
            return Time(func, timerName, -1, memberName, lineNumber);
        }

        [DebuggerStepThrough]
        public T Time<T>(long filterTime, Func<T> func, [CallerMemberName] string memberName = "", [CallerLineNumber] int lineNumber = 0)
        {
            return Time(func, null, filterTime, memberName, lineNumber);
        }

        [DebuggerStepThrough]
        public T Time<T>(string timerName, long filterTime, Func<T> func, [CallerMemberName] string memberName = "", [CallerLineNumber] int lineNumber = 0)
        {
            return Time(func, timerName, filterTime, memberName, lineNumber);
        }

        [DebuggerStepThrough]
        private void Time(Action action, string timerName, long filterTime, string memberName, int lineNumber)
        {
            if (IsEnabled)
            {
                var stopwatch = Stopwatch.StartNew();

                action();

                stopwatch.Stop();
                WriteLine(stopwatch, timerName, filterTime, memberName, lineNumber);
            }
            else
            {
                action();
            }
        }

        [DebuggerStepThrough]
        private T Time<T>(Func<T> func, string timerName, long filterTime, string memberName, int lineNumber)
        {
            if (IsEnabled)
            {
                var stopwatch = Stopwatch.StartNew();

                var result = func();

                stopwatch.Stop();
                WriteLine(stopwatch, timerName, filterTime, memberName, lineNumber);

                return result;
            }
            else
            {
                return func();
            }
        }

        [DebuggerStepThrough]
        private void WriteLine(Stopwatch stopwatch, string timerName, long filterTime, string memberName, int lineNumber)
        {
            if (IsEnabled)
            {
                if (timerName != null)
                {
                    LogIf(stopwatch.ElapsedMilliseconds > filterTime, $"\t[TIMING] {timerName}:{memberName}:{lineNumber} took {stopwatch.Elapsed.TotalSeconds:N6} s");
                }
                else
                {
                    LogIf(stopwatch.ElapsedMilliseconds > filterTime, $"\t[TIMING] {memberName}:{lineNumber} took {stopwatch.Elapsed.TotalSeconds:N6} s");
                }
            }
        }

        [DebuggerStepThrough]
        private void Log(string line)
        {
            logger.Info(line);
        }

        [DebuggerStepThrough]
        private void LogIf(bool condition, string line)
        {
            if (condition)
                Log(line);
        }
    }
}
