using BitSharp.Common.ExtensionMethods;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Common
{
    public class AverageMeasure : IDisposable
    {
        private readonly ReaderWriterLockSlim rwLock;

        private readonly CancellationTokenSource cancelToken;
        private readonly Stopwatch stopwatch;
        private List<Sample> samples;
        private int tickCount;
        private float tickValue;
        private readonly Task sampleTask;
        private bool isDisposed;

        public AverageMeasure(TimeSpan? sampleCutoff = null, TimeSpan? sampleResolution = null)
        {
            this.rwLock = new ReaderWriterLockSlim();
            this.cancelToken = new CancellationTokenSource();
            this.stopwatch = Stopwatch.StartNew();
            this.samples = new List<Sample> { new Sample { SampleStart = TimeSpan.Zero, TickCount = 0, TickValue = 0 } };

            this.SampleCutoff = sampleCutoff ?? TimeSpan.FromSeconds(30);
            this.SampleResolution = sampleResolution ?? TimeSpan.FromSeconds(1);

            this.sampleTask = Task.Run((Func<Task>)this.SampleThread);
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

        public void Tick(float value)
        {
            this.rwLock.DoWrite(() =>
            {
                Interlocked.Increment(ref this.tickCount);
                InterlockedAdd(ref this.tickValue, value);
            });
        }

        public float GetAverage()
        {
            return this.rwLock.DoRead(() =>
            {
                if (this.samples.Count == 0)
                    return 0f;

                var start = this.samples[0].SampleStart;
                var now = stopwatch.Elapsed;

                var cutoff = now - this.SampleCutoff;
                var validSamples = this.samples.Where(x => x.SampleStart >= cutoff).ToList();

                var totalTickCount = validSamples.Sum(x => x.TickCount) + this.tickCount;
                if (totalTickCount == 0)
                    return 0f;

                var totalTickValue = validSamples.Sum(x => x.TickValue) + this.tickValue;

                return totalTickValue / totalTickCount;
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
                    var tickDurationLocal = Interlocked.Exchange(ref this.tickValue, 0);

                    while (this.samples.Count > 0 && this.samples[0].SampleStart < cutoff)
                        this.samples.RemoveAt(0);
                    this.samples.Add(new Sample { SampleStart = start, TickCount = tickCountLocal, TickValue = tickDurationLocal });
                });
            }
        }

        private sealed class Sample
        {
            public TimeSpan SampleStart { get; set; }
            public int TickCount { get; set; }
            public float TickValue { get; set; }
        }

        private static float InterlockedAdd(ref float location1, float value)
        {
            float newCurrentValue = 0;
            while (true)
            {
                float currentValue = newCurrentValue;
                float newValue = currentValue + value;
                newCurrentValue = Interlocked.CompareExchange(ref location1, newValue, currentValue);
                if (newCurrentValue == currentValue)
                    return newValue;
            }
        }
    }
}
