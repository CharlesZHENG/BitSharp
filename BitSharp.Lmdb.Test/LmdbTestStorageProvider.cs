using BitSharp.Core.Storage;
using BitSharp.Core.Test.Storage;

namespace BitSharp.Lmdb.Test
{
    public class LmdbTestStorageProvider : BaseTestStorageProvider, ITestStorageProvider
    {
        public override string Name { get { return "Lmdb Storage"; } }

        public override IStorageManager OpenStorageManager()
        {
            return new LmdbStorageManager(TestDirectory);
        }
    }
}
