using BitSharp.Core.Domain;

namespace BitSharp.Core.Rules
{
    public interface ICoreRules
    {
        //TODO
        bool IgnoreScripts { get; set; }
        bool IgnoreSignatures { get; set; }
        bool IgnoreScriptErrors { get; set; }

        IChainParams ChainParams { get; }

        void PreValidateBlock(Chain chain, ChainedHeader chainedHeader);

        void PostValidateBlock(Chain chain, ChainedHeader chainedHeader, Transaction coinbaseTx, ulong totalTxInputValue, ulong totalTxOutputValue);

        void ValidateTransaction(ChainedHeader chainedHeader, ValidatableTx validatableTx);

        void ValidationTransactionScript(ChainedHeader chainedHeader, BlockTx tx, TxInput txInput, int txInputIndex, PrevTxOutput prevTxOutput);
    }
}