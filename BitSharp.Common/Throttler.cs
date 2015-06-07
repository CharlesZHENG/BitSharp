using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace BitSharp.Common
{
    public static class Throttler
    {
        private static readonly ConcurrentDictionary<string, DateTime> lastTimes = new ConcurrentDictionary<string, DateTime>();

        public static bool IfElapsed(TimeSpan interval, Action action, [CallerMemberName] string memberName = "", [CallerLineNumber] int lineNumber = 0)
        {
            var key = memberName + "_" + lineNumber;

            DateTime lastTime;
            if (!lastTimes.TryGetValue(key, out lastTime))
                lastTime = DateTime.MinValue;

            var now = DateTime.Now;

            if (now - lastTime >= interval)
            {
                lastTimes[key] = now;
                action();
                return true;
            }
            else
                return false;
        }
    }
}
