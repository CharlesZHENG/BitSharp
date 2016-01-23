using BitSharp.Common;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using BitSharp.Core.Storage.Memory;
using BitSharp.Core.Test.Rules;
using BitSharp.Core.Workers;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Ninject;
using Ninject.Parameters;
using System;
using System.Collections.Immutable;
using System.Threading;

namespace BitSharp.Core.Test.Workers
{
    [TestClass]
    public class TargetChainWorkerTest
    {
        [TestMethod]
        public void TestSimpleChain()
        {
            // prepare test kernel
            var kernel = new StandardKernel(new ConsoleLoggingModule(), new MemoryStorageModule());
            kernel.Bind<CoreStorage>().ToSelf().InSingletonScope();
            var coreStorage = kernel.Get<CoreStorage>();

            // initialize data
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.Genesis();
            var header1 = fakeHeaders.Next();
            var header2 = fakeHeaders.Next();

            // store genesis block
            var chainedHeader0 = ChainedHeader.CreateForGenesisBlock(header0);
            coreStorage.AddGenesisBlock(chainedHeader0);

            // mock chain params
            var mockChainParams = new Mock<IChainParams>();
            mockChainParams.Setup(rules => rules.GenesisChainedHeader).Returns(chainedHeader0);
            kernel.Bind<IChainParams>().ToConstant(mockChainParams.Object);

            // initialize the target chain worker
            using (var targetChainWorker = kernel.Get<TargetChainWorker>(new ConstructorArgument("workerConfig", new WorkerConfig(initialNotify: true, minIdleTime: TimeSpan.Zero, maxIdleTime: TimeSpan.MaxValue))))
            {
                // monitor event firing
                var targetChainChangedEvent = new AutoResetEvent(false);
                var onTargetChainChangedCount = 0;
                targetChainWorker.OnTargetChainChanged += () => { onTargetChainChangedCount++; targetChainChangedEvent.Set(); };

                // start worker and wait for initial chain
                targetChainWorker.Start();
                targetChainChangedEvent.WaitOne();

                // verify chained to block 0
                Assert.AreEqual(chainedHeader0, targetChainWorker.TargetChain.LastBlock);
                AssertBlockListEquals(new[] { chainedHeader0 }, targetChainWorker.TargetChain.Blocks);
                Assert.AreEqual(1, onTargetChainChangedCount);

                // add block 1
                ChainedHeader chainedHeader1;
                coreStorage.TryChainHeader(header1, out chainedHeader1);

                // wait for worker
                targetChainChangedEvent.WaitOne();

                // verify chained to block 1
                Assert.AreEqual(chainedHeader1, targetChainWorker.TargetChain.LastBlock);
                AssertBlockListEquals(new[] { chainedHeader0, chainedHeader1 }, targetChainWorker.TargetChain.Blocks);
                Assert.AreEqual(2, onTargetChainChangedCount);

                // add block 2
                ChainedHeader chainedHeader2;
                coreStorage.TryChainHeader(header2, out chainedHeader2);

                // wait for worker
                targetChainChangedEvent.WaitOne();

                // verify chained to block 2
                Assert.AreEqual(chainedHeader2, targetChainWorker.TargetChain.LastBlock);
                AssertBlockListEquals(new[] { chainedHeader0, chainedHeader1, chainedHeader2 }, targetChainWorker.TargetChain.Blocks);
                Assert.AreEqual(3, onTargetChainChangedCount);

                // verify no other work was done
                Assert.IsFalse(targetChainChangedEvent.WaitOne(50));
            }
        }

        [TestMethod]
        public void TestTargetChainReorganize()
        {
            // prepare test kernel
            var kernel = new StandardKernel(new ConsoleLoggingModule(), new MemoryStorageModule());
            kernel.Bind<CoreStorage>().ToSelf().InSingletonScope();
            var coreStorage = kernel.Get<CoreStorage>();

            // initialize data
            var fakeHeaders = new FakeHeaders();
            var header0 = fakeHeaders.Genesis();
            var header1 = fakeHeaders.Next();
            var header2 = fakeHeaders.Next();

            var fakeHeadersA = new FakeHeaders(fakeHeaders);
            var header3A = fakeHeadersA.Next();
            var header4A = fakeHeadersA.Next();
            var header5A = fakeHeadersA.Next();

            var fakeHeadersB = new FakeHeaders(fakeHeaders);
            var header3B = fakeHeadersB.Next();
            var header4B = fakeHeadersB.Next(DataCalculator.ToCompact(UnitTestParams.Target2));

            // store genesis block
            var chainedHeader0 = ChainedHeader.CreateForGenesisBlock(header0);
            coreStorage.AddGenesisBlock(chainedHeader0);

            // mock chain params
            var mockChainParams = new Mock<IChainParams>();
            mockChainParams.Setup(rules => rules.GenesisChainedHeader).Returns(chainedHeader0);
            kernel.Bind<IChainParams>().ToConstant(mockChainParams.Object);

            // initialize the target chain worker
            using (var targetChainWorker = kernel.Get<TargetChainWorker>(new ConstructorArgument("workerConfig", new WorkerConfig(initialNotify: true, minIdleTime: TimeSpan.Zero, maxIdleTime: TimeSpan.MaxValue))))
            {
                // monitor event firing
                var targetChainChangedEvent = new AutoResetEvent(false);
                var onTargetChainChangedCount = 0;
                targetChainWorker.OnTargetChainChanged += () => { onTargetChainChangedCount++; targetChainChangedEvent.Set(); };

                // start worker and wait for initial chain
                targetChainWorker.Start();
                targetChainChangedEvent.WaitOne();

                // verify chained to block 0
                Assert.AreEqual(chainedHeader0, targetChainWorker.TargetChain.LastBlock);
                AssertBlockListEquals(new[] { chainedHeader0 }, targetChainWorker.TargetChain.Blocks);
                Assert.AreEqual(1, onTargetChainChangedCount);

                // add block 1
                ChainedHeader chainedHeader1;
                coreStorage.TryChainHeader(header1, out chainedHeader1);

                // wait for worker
                targetChainChangedEvent.WaitOne();

                // verify chained to block 1
                Assert.AreEqual(chainedHeader1, targetChainWorker.TargetChain.LastBlock);
                AssertBlockListEquals(new[] { chainedHeader0, chainedHeader1 }, targetChainWorker.TargetChain.Blocks);
                Assert.AreEqual(2, onTargetChainChangedCount);

                // add block 2
                ChainedHeader chainedHeader2;
                coreStorage.TryChainHeader(header2, out chainedHeader2);

                // wait for worker
                targetChainChangedEvent.WaitOne();

                // verify chained to block 2
                Assert.AreEqual(chainedHeader2, targetChainWorker.TargetChain.LastBlock);
                AssertBlockListEquals(new[] { chainedHeader0, chainedHeader1, chainedHeader2 }, targetChainWorker.TargetChain.Blocks);
                Assert.AreEqual(3, onTargetChainChangedCount);

                // add block 3A
                ChainedHeader chainedHeader3A;
                coreStorage.TryChainHeader(header3A, out chainedHeader3A);

                // wait for worker
                targetChainChangedEvent.WaitOne();

                // verify chained to block 3A
                Assert.AreEqual(chainedHeader3A, targetChainWorker.TargetChain.LastBlock);
                AssertBlockListEquals(new[] { chainedHeader0, chainedHeader1, chainedHeader2, chainedHeader3A }, targetChainWorker.TargetChain.Blocks);
                Assert.AreEqual(4, onTargetChainChangedCount);

                // add block 4A
                ChainedHeader chainedHeader4A;
                coreStorage.TryChainHeader(header4A, out chainedHeader4A);

                // wait for worker
                targetChainChangedEvent.WaitOne();

                // verify chained to block 4A
                Assert.AreEqual(chainedHeader4A, targetChainWorker.TargetChain.LastBlock);
                AssertBlockListEquals(new[] { chainedHeader0, chainedHeader1, chainedHeader2, chainedHeader3A, chainedHeader4A }, targetChainWorker.TargetChain.Blocks);
                Assert.AreEqual(5, onTargetChainChangedCount);

                // add block 5A
                ChainedHeader chainedHeader5A;
                coreStorage.TryChainHeader(header5A, out chainedHeader5A);

                // wait for worker
                targetChainChangedEvent.WaitOne();

                // verify chained to block 5A
                Assert.AreEqual(chainedHeader5A, targetChainWorker.TargetChain.LastBlock);
                AssertBlockListEquals(new[] { chainedHeader0, chainedHeader1, chainedHeader2, chainedHeader3A, chainedHeader4A, chainedHeader5A }, targetChainWorker.TargetChain.Blocks);
                Assert.AreEqual(6, onTargetChainChangedCount);

                // add block 3B
                ChainedHeader chainedHeader3B;
                coreStorage.TryChainHeader(header3B, out chainedHeader3B);

                // wait for worker, it should not fire
                Assert.IsFalse(targetChainChangedEvent.WaitOne(50));

                // verify no chaining done
                Assert.AreEqual(chainedHeader5A, targetChainWorker.TargetChain.LastBlock);
                AssertBlockListEquals(new[] { chainedHeader0, chainedHeader1, chainedHeader2, chainedHeader3A, chainedHeader4A, chainedHeader5A }, targetChainWorker.TargetChain.Blocks);
                Assert.AreEqual(6, onTargetChainChangedCount);

                // add block 4B
                ChainedHeader chainedHeader4B;
                coreStorage.TryChainHeader(header4B, out chainedHeader4B);

                // wait for worker
                targetChainChangedEvent.WaitOne();

                // verify chained to block 4B
                Assert.AreEqual(chainedHeader4B, targetChainWorker.TargetChain.LastBlock);
                AssertBlockListEquals(new[] { chainedHeader0, chainedHeader1, chainedHeader2, chainedHeader3B, chainedHeader4B }, targetChainWorker.TargetChain.Blocks);
                Assert.AreEqual(7, onTargetChainChangedCount);

                // verify no other work was done
                Assert.IsFalse(targetChainChangedEvent.WaitOne(50));
            }
        }

        private static void AssertBlockListEquals(ChainedHeader[] expected, IImmutableList<ChainedHeader> actual)
        {
            Assert.AreEqual(expected.Length, actual.Count);
            for (var i = 0; i < actual.Count; i++)
                Assert.AreEqual(expected[i], actual[i]);
        }
    }

    internal static class TargetChainWorkerTest_ExtensionMethods
    {
        public static void WaitOneOrFail(this WaitHandle handle, int millisecondsTimeout)
        {
            if (!handle.WaitOne(millisecondsTimeout))
                Assert.Fail("WaitHandle hung");
        }
    }
}
