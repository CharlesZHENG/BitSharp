using BitSharp.Esent.Test;
using BitSharp.LevelDb.Test;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace BitSharp.Core.Test.Storage
{
    [TestClass]
    public class StorageProviderTest
    {
        private readonly List<ITestStorageProvider> testStorageProviders =
            new List<ITestStorageProvider>
            {
                new EsentTestStorageProvider(),
                new MemoryTestStorageProvider(),
                new LevelDbTestStorageProvider(),
            };

        // Run the specified test method against all providers
        protected void RunTest(Action<ITestStorageProvider> testMethod)
        {
            foreach (var provider in testStorageProviders)
            {
                Debug.WriteLine($"Testing provider: {provider.Name}");

                provider.TestInitialize();
                try
                {
                    testMethod(provider);
                }
                finally
                {
                    provider.TestCleanup();
                }
            }
        }
    }
}
