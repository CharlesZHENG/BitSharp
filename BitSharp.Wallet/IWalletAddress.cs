using BitSharp.Common;
using BitSharp.Core.Domain;
using System.Collections.Generic;

namespace BitSharp.Wallet
{
    public interface IWalletAddress
    {
        IEnumerable<UInt256> GetOutputScriptHashes();

        // determine if a tx output matches this wallet address
        bool IsMatcher { get; }
        bool MatchesTxOutput(TxOutput txOutput, UInt256 txOutputScriptHash);
    }
}
