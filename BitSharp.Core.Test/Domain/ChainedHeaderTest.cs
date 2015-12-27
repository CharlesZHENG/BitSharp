using BitSharp.Core.Domain;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace BitSharp.Core.Test.Domain
{
    [TestClass]
    public class ChainedHeaderTest
    {
        [TestMethod]
        public void TestChainedHeaderEquality()
        {
            var randomChainedHeader = RandomData.RandomChainedHeader();
            var randomHeader = RandomData.RandomBlockHeader();

            var sameChainedHeader = new ChainedHeader
            (
                blockHeader: randomChainedHeader.BlockHeader,
                height: randomChainedHeader.Height,
                totalWork: randomChainedHeader.TotalWork,
                dateSeen: randomChainedHeader.DateSeen
            );

            var differentChainedHeaderBlockHeader = new ChainedHeader
            (
                blockHeader: randomHeader,
                height: randomChainedHeader.Height,
                totalWork: randomChainedHeader.TotalWork,
                dateSeen: randomChainedHeader.DateSeen
            );

            var differentChainedHeaderHeight = new ChainedHeader
            (
                blockHeader: randomChainedHeader.BlockHeader,
                height: ~randomChainedHeader.Height,
                totalWork: randomChainedHeader.TotalWork,
                dateSeen: randomChainedHeader.DateSeen
            );

            var differentChainedHeaderTotalWork = new ChainedHeader
            (
                blockHeader: randomChainedHeader.BlockHeader,
                height: randomChainedHeader.Height,
                totalWork: ~randomChainedHeader.TotalWork,
                dateSeen: randomChainedHeader.DateSeen
            );

            var differentChainedHeaderDateSeen = new ChainedHeader
            (
                blockHeader: randomChainedHeader.BlockHeader,
                height: randomChainedHeader.Height,
                totalWork: randomChainedHeader.TotalWork,
                dateSeen: randomChainedHeader.DateSeen.AddMinutes(1)
            );

            Assert.IsTrue(randomChainedHeader.Equals(sameChainedHeader));
            Assert.IsTrue(randomChainedHeader == sameChainedHeader);
            Assert.IsFalse(randomChainedHeader != sameChainedHeader);

            Assert.IsFalse(randomChainedHeader.Equals(differentChainedHeaderBlockHeader));
            Assert.IsFalse(randomChainedHeader == differentChainedHeaderBlockHeader);
            Assert.IsTrue(randomChainedHeader != differentChainedHeaderBlockHeader);

            Assert.IsFalse(randomChainedHeader.Equals(differentChainedHeaderHeight));
            Assert.IsFalse(randomChainedHeader == differentChainedHeaderHeight);
            Assert.IsTrue(randomChainedHeader != differentChainedHeaderHeight);

            Assert.IsFalse(randomChainedHeader.Equals(differentChainedHeaderTotalWork));
            Assert.IsFalse(randomChainedHeader == differentChainedHeaderTotalWork);
            Assert.IsTrue(randomChainedHeader != differentChainedHeaderTotalWork);

            Assert.IsFalse(randomChainedHeader.Equals(differentChainedHeaderDateSeen));
            Assert.IsFalse(randomChainedHeader == differentChainedHeaderDateSeen);
            Assert.IsTrue(randomChainedHeader != differentChainedHeaderDateSeen);
        }
    }
}
