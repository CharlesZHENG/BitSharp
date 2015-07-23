using BitSharp.Common;
using BitSharp.Core.Domain;
using BitSharp.Core.Script;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace BitSharp.Wallet.Address
{
    public class PublicKeyHashAddress : IWalletAddress
    {
        private ImmutableArray<byte> publicKeyHashBytes;
        private UInt256 outputScriptHash;

        public PublicKeyHashAddress(ImmutableArray<byte> publicKeyHashBytes)
        {
            this.publicKeyHashBytes = publicKeyHashBytes;

            var outputScript = new PayToPublicKeyHashBuilder().CreateOutputFromPublicKeyHash(publicKeyHashBytes.ToArray());
            this.outputScriptHash = new UInt256(SHA256Static.ComputeHash(outputScript));
        }

        public IEnumerable<UInt256> GetOutputScriptHashes()
        {
            yield return this.outputScriptHash;
        }

        public bool IsMatcher => false;

        public bool MatchesTxOutput(TxOutput txOutput, UInt256 txOutputScriptHash)
        {
            throw new NotSupportedException();
        }
    }
}
