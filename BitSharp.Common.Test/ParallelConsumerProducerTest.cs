using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reactive;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Common.Test
{
    [TestClass]
    public class ParallelConsumerProducerTest
    {
        /// <summary>
        /// Verify that ParallelConsumerProducer doesn't hang on disposal when a pending consume+produce is outstanding.
        /// </summary>
        [TestMethod]
        [Timeout(20000)]
        public void TestUnconsumedDisposal()
        {
            using (var consumerProducer = new ParallelConsumerProducer<int, long>("", 4))
            using (var sourceReader = new ParallelReader<int>(""))
            using (var source = new BlockingCollection<int>())
            {
                var sourceTask = sourceReader.ReadAsync(source.GetConsumingEnumerable());
                // at minimum complete the BlockingCollection, it will hang the reader thread
                source.CompleteAdding();

                // begin consume-producing, without consuming what's produced
                var consumeProduceTask = consumerProducer.ConsumeProduceAsync(sourceReader,
                    Observer.Create<int>(x => { }),
                    x => new long[0]);
            }
        }

        [TestMethod]
        public void TestConsumeProduce()
        {
            using (var consumerProducer = new ParallelConsumerProducer<int, long>("", 4))
            using (var sourceReader = new ParallelReader<int>(""))
            using (var source = new BlockingCollection<int>())
            {
                var sourceTask = sourceReader.ReadAsync(source.GetConsumingEnumerable());

                var actualConsumed = new List<int>();
                var consumeProduceTask = consumerProducer.ConsumeProduceAsync(sourceReader,
                    // track consumed items
                    Observer.Create<int>(x => actualConsumed.Add(x)),
                    // produce a list from x*2 and x*2+1
                    x => new[] { (long)x * 2, x * 2 + 1 });

                // add source items and generated expected results
                var count = 50;
                var expectedProduced = new long[count * 2];
                for (var i = 0; i < count; i++)
                {
                    source.Add(i);
                    expectedProduced[i * 2] = i * 2;
                    expectedProduced[i * 2 + 1] = i * 2 + 1;
                }

                source.CompleteAdding();

                // verify produced items
                var actualProduced = consumerProducer.GetConsumingEnumerable().ToArray();
                CollectionAssert.AreEquivalent(expectedProduced, actualProduced);

                // verify consumed items
                CollectionAssert.AreEquivalent(Enumerable.Range(0, count).ToArray(), actualConsumed);

                Assert.IsTrue(consumeProduceTask.Wait(2000));
            }
        }

        [TestMethod]
        public void TestSourceException()
        {
            using (var consumerProducer = new ParallelConsumerProducer<int, long>("", 4))
            using (var sourceReader = new ParallelReader<int>(""))
            {
                var zero = 0;
                var source = Enumerable.Range(0, 1).Select(x => 1 / zero);
                var sourceTask = sourceReader.ReadAsync(source);

                var actualConsumed = new List<int>();
                var consumeProduceTask = consumerProducer.ConsumeProduceAsync(sourceReader,
                    Observer.Create<int>(x => { }),
                    x => new long[0]);

                Assert.AreEqual(0, consumerProducer.GetConsumingEnumerable().Count());

                try
                {
                    consumeProduceTask.Wait(2000);
                    Assert.Fail("Source exception not thrown.");
                }
                catch (AggregateException ex)
                {
                    Assert.IsTrue(ex.InnerExceptions[0] is DivideByZeroException);
                    Assert.IsTrue(consumeProduceTask.IsFaulted);
                }
            }
        }
    }
}
