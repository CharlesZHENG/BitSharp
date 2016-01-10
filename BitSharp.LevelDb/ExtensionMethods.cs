using LevelDB;
using System.Reflection;

namespace BitSharp.LevelDb
{
    internal static class ExtensionMethods
    {
        private static readonly MethodInfo writeBatchFinalizeMethod =
            typeof(WriteBatch).GetMethod("Dispose", BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly);

        public static void Dispose(this WriteBatch writeBatch)
        {
            writeBatchFinalizeMethod.Invoke(writeBatch, new object[] { true });
        }
    }
}
