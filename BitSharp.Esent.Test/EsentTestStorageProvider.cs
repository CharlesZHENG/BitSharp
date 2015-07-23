using BitSharp.Core.Storage;
using BitSharp.Core.Test.Storage;

namespace BitSharp.Esent.Test
{
    public class EsentTestStorageProvider : BaseTestStorageProvider, ITestStorageProvider
    {
        static EsentTestStorageProvider()
        {
            EsentStorageManager.InitSystemParameters();
        }

        public override string Name => "Esent Storage";

        public override IStorageManager OpenStorageManager()
        {
            return new EsentStorageManager(TestDirectory);
        }
    }
}
