using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Diagnostics;
using System.Threading;

namespace BitSharp.Common.Test
{
    [TestClass]
    public class RateMeasureTest
    {
        [TestMethod]
        public void TestRateMeasure()
        {
            // start timing
            var stopwatch = Stopwatch.StartNew();

            var sampleCutoff = TimeSpan.FromSeconds(1);
            var sampleResolution = TimeSpan.FromMilliseconds(10);
            using (var rateMeasure = new RateMeasure(sampleCutoff, sampleResolution))
            {
                // tick once per duration, count times
                var count = 10;
                var duration = TimeSpan.FromMilliseconds(20);
                for (var i = 0; i < count; i++)
                {
                    Thread.Sleep(duration);
                    rateMeasure.Tick();
                }

                // stop timing
                stopwatch.Stop();

                // the average rate, per the amount of the time the test has run for, should be as close as possible to count
                Assert.AreEqual(count, rateMeasure.GetAverage(stopwatch.Elapsed), 0.5);

                // wait for the cutoff time to pass and verify the rate dropped to 0
                Thread.Sleep(duration + rateMeasure.SampleCutoff);
                Assert.AreEqual(0, rateMeasure.GetAverage(duration));
            }
        }
    }
}
