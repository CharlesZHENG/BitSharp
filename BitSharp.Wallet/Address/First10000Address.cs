using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Script;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace BitSharp.Wallet.Address
{
    public class First10000Address : IWalletAddress
    {
        public IEnumerable<UInt256> GetOutputScriptHashes()
        {
            using (var stream = this.GetType().Assembly.GetManifestResourceStream("BitSharp.Wallet.Address.First10000.txt"))
            using (var reader = new StreamReader(stream, Encoding.UTF8))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    if (line == "")
                        continue;

                    var bytes = line.HexToByteArray();
                    if (bytes.Length == 65)
                        yield return PublicKeyToOutputScriptHash(bytes);
                    else if (bytes.Length == 20)
                        yield return PublicKeyHashToOutputScriptHash(bytes);
                    else
                        throw new InvalidOperationException();
                }
            }
        }

        public bool IsMatcher => false;

        public bool MatchesTxOutput(TxOutput txOutput, Common.UInt256 txOutputScriptHash)
        {
            throw new NotSupportedException();
        }

        public IEnumerable<IWalletAddress> ToWalletAddresses()
        {
            foreach (var outputScriptHash in this.GetOutputScriptHashes())
                yield return new OutputScriptHashAddress(outputScriptHash);
        }

        private UInt256 PublicKeyToOutputScriptHash(byte[] publicKey)
        {
            if (publicKey.Length != 65)
                throw new ArgumentException("publicKey");

            var outputScript = new PayToPublicKeyBuilder().CreateOutput(publicKey);
            var outputScriptHash = new UInt256(SHA256Static.ComputeHash(outputScript));

            return outputScriptHash;
        }

        private UInt256 PublicKeyHashToOutputScriptHash(byte[] publicKeyHash)
        {
            if (publicKeyHash.Length != 20)
                throw new ArgumentException("publicKeyHash");

            var outputScript = new PayToPublicKeyHashBuilder().CreateOutputFromPublicKeyHash(publicKeyHash);
            var outputScriptHash = new UInt256(SHA256Static.ComputeHash(outputScript));

            return outputScriptHash;
        }
    }
}
