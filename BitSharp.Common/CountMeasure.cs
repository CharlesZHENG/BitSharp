using BitSharp.Common.ExtensionMethods;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Common
{
    public class CountMeasure : IDisposable
    {
        private readonly ReaderWriterLockSlim rwLock;

        private readonly CancellationTokenSource cancelToken;
        private readonly Stopwatch stopwatch;
        private List<Sample> samples;
        private int tickCount;
        private Task sampleTask;
        private bool isDisposed;

        public CountMeasure(TimeSpan sampleCutoff, TimeSpan? sampleResolution = null)
        {
            this.rwLock = new ReaderWriterLockSlim();
            this.cancelToken = new CancellationTokenSource();
            this.stopwatch = Stopwatch.StartNew();
            this.samples = new List<Sample> { new Sample { SampleStart = TimeSpan.Zero, TickCount = 0 } };

            this.SampleCutoff = sampleCutoff;
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

        public int GetCount()
        {
            return this.rwLock.DoRead(() =>
            {
                if (this.samples.Count == 0)
                    return 0;

                var start = this.samples[0].SampleStart;
                var now = stopwatch.Elapsed;

                var totalTickCount = this.samples.Sum(x => x.TickCount) + this.tickCount;

                return totalTickCount;
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
                var cutoff = now - this.SampleCutoff;

                this.rwLock.DoWrite(() =>
                {
                    var tickCountLocal = Interlocked.Exchange(ref this.tickCount, 0);

                    while (this.samples.Count > 0 && this.samples[0].SampleStart < cutoff)
                        this.samples.RemoveAt(0);
                    this.samples.Add(new Sample { SampleStart = start, TickCount = tickCountLocal });
                });
            }
        }

        private sealed class Sample
        {
            public TimeSpan SampleStart { get; set; }
            public int TickCount { get; set; }
        }
    }
}
