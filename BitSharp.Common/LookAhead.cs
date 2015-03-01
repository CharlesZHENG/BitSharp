using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Common
{
    public static class LookAheadMethods
    {
        public static IEnumerable<T> LookAhead<T>(this IEnumerable<T> values, int lookAhead, CancellationToken? cancelToken = null)
        {
            var doneConsuming = false;

            using (var readValues = new BlockingCollection<T>(1 + lookAhead))
            using (var readTask =
                Task.Run(() =>
                {
                    try
                    {
                        foreach (var value in values)
                        {
                            // cooperative loop
                            if (doneConsuming || (cancelToken != null && cancelToken.Value.IsCancellationRequested))
                                return;

                            readValues.Add(value);
                        }
                    }
                    finally
                    {
                        readValues.CompleteAdding();
                    }
                }))
            {
                try
                {
                    foreach (var value in readValues.GetConsumingEnumerable())
                    {
                        // cooperative loop
                        cancelToken.GetValueOrDefault(CancellationToken.None).ThrowIfCancellationRequested();

                        yield return value;
                    }

                    // ensure a cancellation exception is thrown if the loop exited due to cancelToken
                    cancelToken.GetValueOrDefault(CancellationToken.None).ThrowIfCancellationRequested();
                }
                finally
                {
                    // indicate that consuming is finished so that readTask can exit
                    doneConsuming = true;

                    // clear readValues to ensure readTask doesn't block on readValues.Add(value)
                    readValues.GetConsumingEnumerable().Count();

                    // wait for readTask to finish
                    try
                    {
                        readTask.Wait();
                    }
                    catch (AggregateException e)
                    {
                        throw e.InnerExceptions.First();
                    }
                }
            }
        }
    }
}