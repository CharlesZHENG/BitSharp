using BitSharp.Common.ExtensionMethods;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Common
{
    public class DurationMeasure : IDisposable
    {
        private readonly ReaderWriterLockSlim rwLock;

        private readonly CancellationTokenSource cancelToken;
        private readonly Stopwatch stopwatch;
        private List<Sample> samples;
        private int tickCount;
        private long tickDuration;
        private readonly Task sampleTask;
        private bool isDisposed;

        public DurationMeasure(TimeSpan? sampleCutoff = null, TimeSpan? sampleResolution = null)
        {
            this.rwLock = new ReaderWriterLockSlim();
            this.cancelToken = new CancellationTokenSource();
            this.stopwatch = Stopwatch.StartNew();
            this.samples = new List<Sample> { new Sample { SampleStart = TimeSpan.Zero, TickCount = 0, TickDuration = 0 } };

            this.SampleCutoff = sampleCutoff ?? TimeSpan.FromSeconds(30);
            this.SampleResolution = sampleResolution ?? TimeSpan.FromSeconds(1);

            this.sampleTask = Task.Run((Func<Task>)this.SampleThread);
        }

        public TimeSpan SampleCutoff { get; set; }

        public TimeSpan SampleResolution { get; set; }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!this.isDisposed && disposing)
            {
                this.cancelToken.Cancel();
                this.sampleTask.Wait();

                this.cancelToken.Dispose();
                this.rwLock.Dispose();

                this.isDisposed = true;
            }
        }

        public void Tick(TimeSpan duration)
        {
            this.rwLock.DoWrite(() =>
            {
                Interlocked.Increment(ref this.tickCount);
                Interlocked.Add(ref this.tickDuration, duration.Ticks);
            });
        }

        [DebuggerStepThrough]
        public void Measure(Action action)
        {
            var stopwatch = Stopwatch.StartNew();
            action();
            stopwatch.Stop();

            Tick(stopwatch.Elapsed);
        }

        [DebuggerStepThrough]
        public void MeasureIf(bool condition, Action action)
        {
            if (condition)
                Measure(action);
            else
                action();
        }

        [DebuggerStepThrough]
        public T Measure<T>(Func<T> func)
        {
            var stopwatch = Stopwatch.StartNew();
            var result = func();
            stopwatch.Stop();

            Tick(stopwatch.Elapsed);
            return result;
        }

        public TimeSpan GetAverage()
        {
            return this.rwLock.DoRead(() =>
            {
                if (this.samples.Count == 0)
                    return TimeSpan.Zero;

                var start = this.samples[0].SampleStart;
                TimeSpan now;
                lock (stopwatch)
                    now = stopwatch.Elapsed;

                var cutoff = now - this.SampleCutoff;
                var validSamples = this.samples.Where(x => x.SampleStart >= cutoff).ToList();

                var totalTickCount = validSamples.Sum(x => x.TickCount) + this.tickCount;
                if (totalTickCount == 0)
                    return TimeSpan.Zero;

                var totalTickDuration = validSamples.Sum(x => x.TickDuration) + this.tickDuration;

                return new TimeSpan(totalTickDuration / totalTickCount);
            });
        }

        private async Task SampleThread()
        {
            while (true)
            {
                TimeSpan start;
                lock (stopwatch)
                    start = stopwatch.Elapsed;

                try
                {
                    await Task.Delay(this.SampleResolution, this.cancelToken.Token);
                }
                catch (TaskCanceledException)
                {
                    return;
                }

                TimeSpan now;
                lock (stopwatch)
                    now = stopwatch.Elapsed;
                var cutoff = now - this.SampleCutoff;

                this.rwLock.DoWrite(() =>
                {
                    var tickCountLocal = Interlocked.Exchange(ref this.tickCount, 0);
                    var tickDurationLocal = Interlocked.Exchange(ref this.tickDuration, 0);

                    while (this.samples.Count > 0 && this.samples[0].SampleStart < cutoff)
                        this.samples.RemoveAt(0);
                    this.samples.Add(new Sample { SampleStart = start, TickCount = tickCountLocal, TickDuration = tickDurationLocal });
                });
            }
        }

        private sealed class Sample
        {
            public TimeSpan SampleStart { get; set; }
            public int TickCount { get; set; }
            public long TickDuration { get; set; }
        }
    }
}
