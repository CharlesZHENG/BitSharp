using System;
using System.IO;

namespace BitSharp.Node
{
    public static class Config
    {
        private static readonly string _localStoragePath;

        static Config()
        {
            _localStoragePath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "BitSharp");
        }

        public static string LocalStoragePath { get { return _localStoragePath; } }
    }
}
