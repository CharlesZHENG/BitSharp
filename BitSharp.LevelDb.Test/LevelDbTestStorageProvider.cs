using BitSharp.Core.Storage;
using BitSharp.Core.Test.Storage;

namespace BitSharp.LevelDb.Test
{
    public class LevelDbTestStorageProvider : BaseTestStorageProvider, ITestStorageProvider
    {
        public override string Name => "LevelDb Storage";

        public override IStorageManager OpenStorageManager()
        {
            return new LevelDbStorageManager(TestDirectory);
        }
    }
}
