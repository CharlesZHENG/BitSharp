using System;
using System.IO;

namespace BitSharp.Node
{
    public static class Config
    {
        static Config()
        {
            LocalStoragePath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "BitSharp");
        }

        public static string LocalStoragePath { get; }
    }
}
