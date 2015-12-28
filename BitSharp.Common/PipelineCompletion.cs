using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BitSharp.Common
{
    public static class PipelineCompletion
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        //TODO
        public static async Task Create(IEnumerable<Task> tasks, IEnumerable<IDataflowBlock> dataFlowBlocks)
        {
            var tasksArray = tasks.Concat(dataFlowBlocks.Select(x => x.Completion)).ToArray();

            var taskExceptions = new ConcurrentBag<Exception>();
            var catchTasks = new Task[tasksArray.Length];

            var finishedEvent = new TaskCompletionSource<object>();
            for (var i = 0; i < tasksArray.Length; i++)
            {
                var task = tasksArray[i];

                catchTasks[i] =
                    task.ContinueWith(_ =>
                    {
                        if (task.IsFaulted)
                        {
                            taskExceptions.Add(task.Exception);
                            throw task.Exception;
                        }
                        else
                        {
                            finishedEvent.Task.Wait();
                        }
                    });
            }

            try
            {
                // wait for any of the tasks to fault, or for all tasks to complete
                var whenAnyThrows = Task.WhenAny(catchTasks);
                var whenAllCompleted = Task.WhenAll(tasks);
                var finishedTask = await Task.WhenAny(whenAnyThrows, whenAllCompleted);

                // propagate exceptions
                finishedTask.Wait();
                foreach (var catchTask in catchTasks)
                    if (catchTask.IsFaulted)
                        catchTask.Wait();

                // we should only be here if all tasks completed successfully
                Debug.Assert(finishedTask == whenAllCompleted);
                Debug.Assert(!finishedEvent.Task.IsCompleted);
                Debug.Assert(!catchTasks.Any(x => x.IsCompleted));
                Debug.Assert(tasks.All(x => x.Status == TaskStatus.RanToCompletion));
            }
            catch (Exception ex)
            {
                var throwException = taskExceptions.Count > 0 ? new AggregateException(taskExceptions) : ex;

                foreach (var dataFlowBlock in dataFlowBlocks)
                    dataFlowBlock.Fault(throwException);

                throw throwException;
            }
            finally
            {
                finishedEvent.SetResult(null);
            }
        }
    }
}
