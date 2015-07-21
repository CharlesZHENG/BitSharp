using System;
using System.Threading.Tasks;

namespace BitSharp.Common
{
    public static class TaskPool
    {
        public static Task Run(int count, Action action)
        {
            var tasks = new Task[count];
            for (var i = 0; i < count; i++)
                tasks[i] = Task.Run(action);

            return Task.WhenAll(tasks);
        }
    }
}
