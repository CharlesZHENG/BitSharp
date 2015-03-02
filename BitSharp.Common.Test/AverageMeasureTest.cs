using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Threading;

namespace BitSharp.Common.Test
{
    [TestClass]
    public class AverageMeasureTest
    {
        [TestMethod]
        public void TestAverageMeasure()
        {
            var sampleCutoff = TimeSpan.FromMilliseconds(5000);
            var sampleResolution = TimeSpan.FromMilliseconds(100);
            using (var averageMeasure = new AverageMeasure(sampleCutoff, sampleResolution))
            {
                // add samples
                var count = 5;
                var value = 10;
                for (var i = 0; i < count; i++)
                {
                    averageMeasure.Tick(value);
                }

                // wait half the cutoff time and verify average
                Thread.Sleep(new TimeSpan(sampleCutoff.Ticks / 2));
                Assert.AreEqual((int)value, (int)averageMeasure.GetAverage());

                // wait for the cutoff time to pass and verify the average dropped to 0
                Thread.Sleep(sampleCutoff);
                Assert.AreEqual(0, (int)averageMeasure.GetAverage());

                // add new samples
                value = 50;
                for (var i = 0; i < count; i++)
                {
                    averageMeasure.Tick(value);
                }

                // verify average
                Assert.AreEqual((int)value, (int)averageMeasure.GetAverage());
            }
        }
    }
}
