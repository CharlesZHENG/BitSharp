using BitSharp.Common;
using BitSharp.Core.Domain;
using System.Collections.Immutable;

namespace BitSharp.Core.Rules
{
    public interface IBlockchainRules
    {
        //TODO
        bool BypassPrevTxLoading { get; set; }
        bool IgnoreScripts { get; set; }
        bool IgnoreSignatures { get; set; }
        bool IgnoreScriptErrors { get; set; }

        UInt256 HighestTarget { get; }

        Block GenesisBlock { get; }

        ChainedHeader GenesisChainedHeader { get; }

        void PreValidateBlock(Chain chain, ChainedHeader chainedHeader);

        void PostValidateBlock(Chain chain, ChainedHeader chainedHeader, Transaction coinbaseTx, ulong totalTxInputValue, ulong totalTxOutputValue);

        void ValidateTransaction(ChainedHeader chainedHeader, ValidatableTx loadedTx);

        void ValidationTransactionScript(ChainedHeader chainedHeader, BlockTx tx, TxInput txInput, int txInputIndex, TxOutput prevTxOutput);
    }
}
