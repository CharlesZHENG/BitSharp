using BitSharp.Core.Rules;

namespace BitSharp.Core.Test
{
    public static class TestBlockProvider
    {
        public static BlockProvider CreateForRules(ChainTypeEnum chainType)
        {
            switch (chainType)
            {
                case ChainTypeEnum.MainNet:
                    return new MainnetBlockProvider();
                case ChainTypeEnum.TestNet3:
                    return new TestNet3BlockProvider();
                default:
                    return null;
            }
        }
    }

    public class MainnetBlockProvider : BlockProvider
    {
        public MainnetBlockProvider()
            : base("BitSharp.Core.Test.Blocks.Mainnet.zip")
        {
        }
    }

    public class TestNet3BlockProvider : BlockProvider
    {
        public TestNet3BlockProvider()
            : base("BitSharp.Core.Test.Blocks.TestNet3.zip")
        {
        }
    }
}
