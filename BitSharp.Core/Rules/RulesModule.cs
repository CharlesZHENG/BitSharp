using Ninject.Modules;

namespace BitSharp.Core.Rules
{
    public enum RulesEnum
    {
        MainNet,
        TestNet2,
        TestNet3,
        ComparisonToolTestNet
    }

    public class RulesModule : NinjectModule
    {
        private readonly RulesEnum rules;

        public RulesModule(RulesEnum rules)
        {
            this.rules = rules;
        }

        public override void Load()
        {
            this.Bind<RulesEnum>().ToConstant(this.rules);

            switch (this.rules)
            {
                case RulesEnum.MainNet:
                    this.Bind<IBlockchainRules>().To<MainnetRules>().InSingletonScope();
                    break;

                case RulesEnum.TestNet2:
                case RulesEnum.ComparisonToolTestNet:
                    this.Bind<IBlockchainRules>().To<Testnet2Rules>().InSingletonScope();
                    break;
                
                case RulesEnum.TestNet3:
                    this.Bind<IBlockchainRules>().To<Testnet3Rules>().InSingletonScope();
                    break;
            }
        }
    }
}
