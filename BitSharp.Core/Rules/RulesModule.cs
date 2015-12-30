using Ninject.Modules;
using System;

namespace BitSharp.Core.Rules
{
    public class RulesModule : NinjectModule
    {
        private readonly ChainType chainType;

        public RulesModule(ChainType chainType)
        {
            this.chainType = chainType;
        }

        public override void Load()
        {
            this.Bind<ChainType>().ToConstant(this.chainType);

            switch (this.chainType)
            {
                case ChainType.MainNet:
                    this.Bind<IChainParams>().To<MainnetParams>().InSingletonScope();
                    break;

                case ChainType.Regtest:
                    this.Bind<IChainParams>().To<RegtestParams>().InSingletonScope();
                    break;

                case ChainType.TestNet3:
                    this.Bind<IChainParams>().To<Testnet3Params>().InSingletonScope();
                    break;

                default:
                    throw new InvalidOperationException();
            }

            this.Bind<ICoreRules>().To<CoreRules>().InSingletonScope();
        }
    }
}
