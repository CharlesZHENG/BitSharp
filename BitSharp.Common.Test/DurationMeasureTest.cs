using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Threading;

namespace BitSharp.Common.Test
{
    [TestClass]
    public class DurationMeasureTest
    {
        [TestMethod]
        public void TestDurationMeasure()
        {
            var sampleCutoff = TimeSpan.FromMilliseconds(5000);
            var sampleResolution = TimeSpan.FromMilliseconds(100);
            using (var durationMeasure = new DurationMeasure(sampleCutoff, sampleResolution))
            {
                // add duration samples
                var count = 5;
                var duration = TimeSpan.FromMilliseconds(10);
                for (var i = 0; i < count; i++)
                {
                    durationMeasure.Tick(duration);
                }

                // wait half the cutoff time and verify average duration
                Thread.Sleep(new TimeSpan(sampleCutoff.Ticks / 2));
                Assert.AreEqual((int)duration.TotalMilliseconds, (int)durationMeasure.GetAverage().TotalMilliseconds);

                // wait for the cutoff time to pass and verify the average duration dropped to 0
                Thread.Sleep(sampleCutoff);
                Assert.AreEqual(0, durationMeasure.GetAverage().TotalSeconds);

                // add new duration samples
                duration = TimeSpan.FromMilliseconds(50);
                for (var i = 0; i < count; i++)
                {
                    durationMeasure.Tick(duration);
                }

                // verify average duration
                Assert.AreEqual(duration.TotalSeconds, durationMeasure.GetAverage().TotalSeconds, 0.01);
            }
        }
    }
}
