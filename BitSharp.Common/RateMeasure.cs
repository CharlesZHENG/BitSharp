using BitSharp.Common.ExtensionMethods;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Common
{
    public class RateMeasure : IDisposable
    {
        private readonly ReaderWriterLockSlim rwLock;

        private readonly CancellationTokenSource cancelToken;
        private readonly Stopwatch stopwatch;
        private List<Sample> samples;
        private int tickCount;
        private Task sampleTask;
        private bool isDisposed;

        public RateMeasure(TimeSpan? sampleCutoff = null, TimeSpan? sampleResolution = null)
        {
            this.rwLock = new ReaderWriterLockSlim();
            this.cancelToken = new CancellationTokenSource();
            this.stopwatch = Stopwatch.StartNew();
            this.samples = new List<Sample> { new Sample { SampleStart = this.stopwatch.Elapsed, SampleDuration = TimeSpan.Zero, TickCount = 0 } };

            this.SampleCutoff = sampleCutoff ?? TimeSpan.FromSeconds(30);
            this.SampleResolution = sampleResolution ?? TimeSpan.FromSeconds(1);

            this.sampleTask = this.SampleThread();
        }

        public TimeSpan SampleCutoff { get; set; }

        public TimeSpan SampleResolution { get; set; }

        public void Dispose()
        {
            if (this.isDisposed)
                return;

            this.cancelToken.Cancel();
            this.sampleTask.Wait();

            this.cancelToken.Dispose();
            this.rwLock.Dispose();

            this.isDisposed = true;
        }

        public void Tick()
        {
            Interlocked.Increment(ref this.tickCount);
        }

        public void Tick(int count)
        {
            Interlocked.Add(ref this.tickCount, count);
        }

        public float GetAverage(TimeSpan? perUnitTime = null)
        {
            if (perUnitTime == null)
                perUnitTime = TimeSpan.FromSeconds(1);

            return this.rwLock.DoRead(() =>
            {
                if (this.samples.Count == 0)
                    return 0;

                var start = this.samples[0].SampleStart;
                var now = stopwatch.Elapsed;

                var duration = now - start;
                if (duration <= TimeSpan.Zero)
                    return 0;

                var cutoff = now - this.SampleCutoff;
                var validSamples = this.samples.Where(x => x.SampleStart >= cutoff).ToList();

                var totalTickCount = validSamples.Sum(x => x.TickCount) + this.tickCount;

                var unitsOfTime = (double)duration.Ticks / perUnitTime.Value.Ticks;

                return (float)(totalTickCount / unitsOfTime);
            });
        }

        private async Task SampleThread()
        {
            while (true)
            {
                var start = stopwatch.Elapsed;

                try
                {
                    await Task.Delay(this.SampleResolution, this.cancelToken.Token);
                }
                catch (TaskCanceledException)
                {
                    return;
                }

                var now = stopwatch.Elapsed;
                var duration = now - start;
                var cutoff = now - this.SampleCutoff;

                this.rwLock.DoWrite(() =>
                {
                    var tickCountLocal = Interlocked.Exchange(ref this.tickCount, 0);

                    while (this.samples.Count > 0 && this.samples[0].SampleStart < cutoff)
                        this.samples.RemoveAt(0);
                    this.samples.Add(new Sample { SampleStart = start, SampleDuration = duration, TickCount = tickCountLocal });
                });
            }
        }

        private sealed class Sample
        {
            public TimeSpan SampleStart { get; set; }
            public TimeSpan SampleDuration { get; set; }
            public int TickCount { get; set; }
        }
    }
}
