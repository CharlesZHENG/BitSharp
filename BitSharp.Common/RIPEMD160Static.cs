using System;
using System.Security.Cryptography;

namespace BitSharp.Common
{
    public static class RIPEMD160Static
    {
        [ThreadStatic]
        private static RIPEMD160Managed ripemd160;

        public static byte[] ComputeHash(byte[] buffer)
        {
            var ripemd160 = GetRIPEMD160();
            return ripemd160.ComputeHash(buffer);
        }

        private static RIPEMD160Managed GetRIPEMD160()
        {
            if (ripemd160 == null)
                ripemd160 = new RIPEMD160Managed();

            return ripemd160;
        }
    }
}
