using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace BitSharp.Common
{
    public static class Throttler
    {
        private static readonly ConcurrentDictionary<Tuple<string, string, int>, DateTimeOffset[]> lastTimes = new ConcurrentDictionary<Tuple<string, string, int>, DateTimeOffset[]>();

        [DebuggerStepThrough]
        public static bool IfElapsed(TimeSpan interval, Action action, [CallerMemberName] string memberName = "", [CallerFilePath] string filePath = "", [CallerLineNumber] int lineNumber = 0)
        {
            var key = Tuple.Create(memberName, filePath, lineNumber);

            var now = DateTimeOffset.Now;
            var lastTime = lastTimes.GetOrAdd(key, new[] { DateTimeOffset.MinValue });

            lock (lastTime)
            {
                if (now - lastTime[0] >= interval)
                {
                    lastTime[0] = now;
                    action();
                    return true;
                }
                else
                    return false;
            }
        }
    }
}
