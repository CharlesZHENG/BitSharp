using LevelDB;

namespace BitSharp.LevelDb
{
    internal static class ExtensionMethods
    {
        public static bool TryGet(this DB db, ReadOptions options, byte[] key, out byte[] value)
        {
            value = db.Get(options, key);
            return value != null;
        }
    }
}
