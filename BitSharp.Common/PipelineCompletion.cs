using NLog;
using System;
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
        public static async Task Create(Task[] tasks, IDataflowBlock[] dataFlowBlocks)
        {
            var pipelineTasks = tasks.Concat(dataFlowBlocks.Select(x => x.Completion)).ToArray();

            var catchTasks = new Task[pipelineTasks.Length];
            for (var i = 0; i < pipelineTasks.Length; i++)
            {
                var task = pipelineTasks[i];

                catchTasks[i] =
                    task.ContinueWith(_ =>
                    {
                        if (task.IsFaulted)
                        {
                            // fault all dataflow blocks as soon as any fault occurs
                            foreach (var dataFlowBlock in dataFlowBlocks)
                                dataFlowBlock.Fault(task.Exception);
                        }
                    });
            }

            try
            {
                var allTasks = pipelineTasks.Concat(catchTasks).ToArray();
                await Task.WhenAll(allTasks);

                // we should only be here if all tasks completed successfully
                Debug.Assert(allTasks.All(x => x.Status == TaskStatus.RanToCompletion));
            }
            catch (Exception ex)
            {
                // ensure all dataflow blocks are faulted
                foreach (var dataFlowBlock in dataFlowBlocks)
                    dataFlowBlock.Fault(ex);

                throw;
            }
        }
    }
}
