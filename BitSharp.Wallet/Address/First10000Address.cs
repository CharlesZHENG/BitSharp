﻿using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Script;
using BitSharp.Wallet.Base58;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

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
                        Debugger.Break();
                }
            }
        }

        public bool IsMatcher
        {
            get { return false; }
        }

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
                Debugger.Break();
            //Debug.Assert(publicKey.Length == 65);

            var outputScript = new PayToPublicKeyBuilder().CreateOutput(publicKey);
            var outputScriptHash = new UInt256(SHA256Static.ComputeHash(outputScript));

            return outputScriptHash;
        }

        private UInt256 PublicKeyHashToOutputScriptHash(byte[] publicKeyHash)
        {
            if (publicKeyHash.Length != 20)
                Debugger.Break();
            //Debug.Assert(publicKeyHash.Length == 20);

            var outputScript = new PayToPublicKeyHashBuilder().CreateOutputFromPublicKeyHash(publicKeyHash);
            var outputScriptHash = new UInt256(SHA256Static.ComputeHash(outputScript));

            return outputScriptHash;
        }
    }
}
