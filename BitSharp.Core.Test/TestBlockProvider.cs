using BitSharp.Common;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Reflection;

namespace BitSharp.Core.Test
{
    public static class TestBlockProvider
    {
        public static BlockProvider CreateForRules(RulesEnum rulesType)
        {
            switch (rulesType)
            {
                case RulesEnum.MainNet:
                    return new MainnetBlockProvider();
                case RulesEnum.TestNet3:
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
