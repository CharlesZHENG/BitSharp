using BitSharp.Common;
using BitSharp.Core.Domain;
using BitSharp.Core.Script;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Wallet.Address
{
    public class PublicKeyAddress : IWalletAddress
    {
        private ImmutableArray<byte> publicKeyBytes;
        private UInt256 outputScript1Hash;
        private UInt256 outputScript2Hash;

        public PublicKeyAddress(ImmutableArray<byte> publicKeyBytes)
        {
            this.publicKeyBytes = publicKeyBytes;

            var outputScript1 = new PayToPublicKeyBuilder().CreateOutput(publicKeyBytes.ToArray());
            var outputScript2 = new PayToPublicKeyHashBuilder().CreateOutputFromPublicKey(publicKeyBytes.ToArray());
            this.outputScript1Hash = new UInt256(SHA256Static.ComputeHash(outputScript1));
            this.outputScript2Hash = new UInt256(SHA256Static.ComputeHash(outputScript2));
        }

        public IEnumerable<UInt256> GetOutputScriptHashes()
        {
            yield return this.outputScript1Hash;
            yield return this.outputScript2Hash;
        }

        public bool IsMatcher => false;

        public bool MatchesTxOutput(TxOutput txOutput, UInt256 txOutputScriptHash)
        {
            throw new NotSupportedException();
        }
    }
}
