using Ninject.Modules;
using System;

namespace BitSharp.Core.Rules
{
    public enum ChainTypeEnum
    {
        MainNet,
        TestNet2,
        TestNet3,
        ComparisonToolTestNet
    }

    public class RulesModule : NinjectModule
    {
        private readonly ChainTypeEnum chainType;

        public RulesModule(ChainTypeEnum chainType)
        {
            this.chainType = chainType;
        }

        public override void Load()
        {
            this.Bind<ChainTypeEnum>().ToConstant(this.chainType);

            switch (this.chainType)
            {
                case ChainTypeEnum.MainNet:
                    this.Bind<IChainParams>().To<MainnetParams>().InSingletonScope();
                    break;

                case ChainTypeEnum.TestNet2:
                case ChainTypeEnum.ComparisonToolTestNet:
                    this.Bind<IChainParams>().To<Testnet2Params>().InSingletonScope();
                    break;

                case ChainTypeEnum.TestNet3:
                    this.Bind<IChainParams>().To<Testnet3Params>().InSingletonScope();
                    break;

                default:
                    throw new InvalidOperationException();
            }

            this.Bind<ICoreRules>().To<CoreRules>().InSingletonScope();
        }
    }
}
