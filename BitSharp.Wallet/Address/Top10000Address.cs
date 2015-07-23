using BitSharp.Common;
using BitSharp.Core.Domain;
using BitSharp.Core.Script;
using BitSharp.Wallet.Base58;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace BitSharp.Wallet.Address
{
    public class Top10000Address : IWalletAddress
    {
        public IEnumerable<UInt256> GetOutputScriptHashes()
        {
            using (var stream = this.GetType().Assembly.GetManifestResourceStream("BitSharp.Wallet.Address.Top10000.txt"))
            using (var reader = new StreamReader(stream, Encoding.UTF8))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    if (line == "")
                        continue;

                    yield return AddressToOutputScriptHash(line);
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

        private UInt256 AddressToOutputScriptHash(string address)
        {
            var addressBytes = Base58Encoding.DecodeWithCheckSum(address);
            var publicKeyHash = addressBytes.Skip(1).ToArray();

            var outputScript = new PayToPublicKeyHashBuilder().CreateOutputFromPublicKeyHash(publicKeyHash);
            var outputScriptHash = new UInt256(SHA256Static.ComputeHash(outputScript));

            return outputScriptHash;
        }
    }
}
