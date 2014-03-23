﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BitSharp.Common.ExtensionMethods;

namespace BitSharp.Common
{
    public static class LookAheadMethods
    {
        public static IEnumerable<T> LookAhead<T>(this IEnumerable<T> values, int maxLookAhead, CancellationToken? cancelToken = null)
        {
            var readValues = new ConcurrentQueue<T>();

            var finished = false;
            var abortToken = new CancellationTokenSource();
            Exception readException = null;

            var valueReadEvent = new AutoResetEvent(true);
            var valueAvailableEvent = new AutoResetEvent(false);
            var thread = new Thread(() =>
            {
                try
                {
                    var currentLookAhead = 1;
                    using (var valuesEnumerator = values.GetEnumerator())
                    {
                        while (!finished)
                        {
                            // cooperative loop
                            cancelToken.GetValueOrDefault(CancellationToken.None).ThrowIfCancellationRequested();
                            abortToken.Token.ThrowIfCancellationRequested();

                            var value = valuesEnumerator.Current;

                            if (readValues.Count < 1 + currentLookAhead)
                            {
                                if (valuesEnumerator.MoveNext())
                                {
                                    readValues.Enqueue(valuesEnumerator.Current);
                                    valueAvailableEvent.Set();
                                }
                                else
                                {
                                    finished = true;
                                }
                            }
                            else
                            {
                                if (currentLookAhead < maxLookAhead
                                    && valueReadEvent.WaitOne(0))
                                {
                                    //Debug.WriteLine("Increasing look ahead to: {0}".Format2(currentLookAhead));
                                    currentLookAhead++;
                                }
                                else
                                {
                                    valueReadEvent.WaitOne();
                                }
                            }
                        }
                    }
                    valueAvailableEvent.Set();
                }
                catch (Exception e)
                {
                    readException = e;
                    valueAvailableEvent.Set();
                }
            });

            thread.Start();
            try
            {
                while (!finished || readValues.Count > 0)
                {
                    // cooperative loop
                    cancelToken.GetValueOrDefault(CancellationToken.None).ThrowIfCancellationRequested();

                    if (!finished)
                    {
                        valueAvailableEvent.WaitOne();
                        if (readException != null)
                            throw readException;
                    }

                    T value;
                    if (readValues.TryDequeue(out value))
                        yield return value;

                    valueReadEvent.Set();
                }
            }
            finally
            {
                abortToken.Cancel();
                valueReadEvent.Set();
            }
        }

        public static IEnumerable<T> LookAhead___<T>(Func<IEnumerable<T>> values, CancellationToken cancelToken)
        {
            // setup task completion sources to read results of look ahead
            using (var resultWriteEvent = new AutoResetEvent(false))
            using (var resultReadEvent = new AutoResetEvent(false))
            using (var internalCancelToken = new CancellationTokenSource())
            {
                var results = new ConcurrentQueue<T>();
                var resultReadIndex = new[] { -1 };
                var targetIndex = new[] { 0 };

                var resultsCount = 0;
                var finalCount = -1;

                var readTimes = new List<DateTime>();
                readTimes.Add(DateTime.UtcNow);

                var exceptions = new ConcurrentBag<Exception>();

                var lookAheadThread = new Thread(() =>
                {
                    try
                    {
                        // look-ahead loop
                        var indexLocal = 0;
                        var valuesLocal = values();
                        foreach (var value in valuesLocal)
                        {
                            // cooperative loop
                            if (internalCancelToken.IsCancellationRequested || (cancelToken != null && cancelToken.IsCancellationRequested))
                            {
                                return;
                            }

                            // store the result and notify
                            results.Enqueue(value);
                            resultsCount++;
                            resultWriteEvent.Set();

                            // make sure look-ahead doesn't go too far ahead, based on calculated index above
                            while (indexLocal > targetIndex[0])
                            {
                                // cooperative loop
                                if (internalCancelToken.IsCancellationRequested || (cancelToken != null && cancelToken.IsCancellationRequested))
                                {
                                    return;
                                }

                                // wait for a result to be read
                                resultReadEvent.WaitOne(TimeSpan.FromMilliseconds(10));
                            }

                            indexLocal++;
                        }

                        // notify done
                        finalCount = resultsCount;
                        resultWriteEvent.Set();
                    }
                    catch (Exception e)
                    {
                        // notify the enumerator loop of any exceptions
                        exceptions.Add(e);
                        resultWriteEvent.Set();
                    }
                });
                lookAheadThread.Name = "LookAhead.{0}".Format2(typeof(T));

                lookAheadThread.Start();
                try
                {
                    // enumerate the results
                    var i = 0;
                    while (finalCount == -1 || i < finalCount)
                    {
                        // cooperative loop
                        if (cancelToken != null)
                            cancelToken.ThrowIfCancellationRequested();

                        // unblock loook-ahead and wait for current result to be come available
                        resultReadEvent.Set();
                        while (i >= resultsCount && (finalCount == -1 || i < finalCount) && exceptions.Count == 0)
                        {
                            // cooperative loop
                            if (cancelToken != null)
                                cancelToken.ThrowIfCancellationRequested();

                            resultWriteEvent.WaitOne(TimeSpan.FromMilliseconds(10));
                        }

                        // check if any exceptions occurred in the look-ahead loop
                        if (exceptions.Count > 0)
                            throw new AggregateException(exceptions);

                        // check if enumration is finished
                        if (finalCount != -1 && i >= finalCount)
                            break;

                        // retrieve current result and clear reference
                        T result;
                        if (!results.TryDequeue(out result))
                            throw new Exception();

                        // update current index and unblock look-ahead
                        resultReadIndex[0] = i;
                        resultReadEvent.Set();
                        i++;

                        // store time the result was read
                        readTimes.Add(DateTime.UtcNow);
                        while (readTimes.Count > 500)
                            readTimes.RemoveAt(0);

                        // calculate how far to look-ahead based on how quickly the results are being read
                        var firstReadTime = readTimes[0];
                        var readPerMillisecond = (float)(readTimes.Count / (DateTime.UtcNow - firstReadTime).TotalMilliseconds);
                        if (float.IsNaN(readPerMillisecond))
                            readPerMillisecond = 0;
                        targetIndex[0] = resultReadIndex[0] + 1 + (int)(readPerMillisecond * 1000); // look ahead 1000 milliseconds

                        // yield result
                        yield return result;
                    }
                }
                finally
                {
                    // ensure look-ahead thread is cleaned up
                    internalCancelToken.Cancel();
                    lookAheadThread.Join();
                }
            }
        }
    }
}