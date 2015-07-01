using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Storage;
using BitSharp.Core.Test.Storage;
using System;
using System.IO;

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
