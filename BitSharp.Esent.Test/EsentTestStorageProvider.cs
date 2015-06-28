using BitSharp.Core.Storage;
using BitSharp.Core.Test.Storage;
using System;
using System.IO;

namespace BitSharp.Esent.Test
{
    public class EsentTestStorageProvider : BaseTestStorageProvider, ITestStorageProvider
    {
        public override string Name { get { return "Esent Storage"; } }

        public override IStorageManager OpenStorageManager()
        {
            return new EsentStorageManager(TestDirectory);
        }
    }
}
