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

        // executed serially, in order
        void TallyTransaction(ChainedHeader chainedHeader, ValidatableTx validatableTx, ref object runningTally);

        // executed in parallel, any order
        void ValidateTransaction(ChainedHeader chainedHeader, ValidatableTx validatableTx);

        void ValidationTransactionScript(ChainedHeader chainedHeader, BlockTx tx, TxInput txInput, int txInputIndex, PrevTxOutput prevTxOutput);

        void PostValidateBlock(Chain chain, ChainedHeader chainedHeader, object finalTally);
    }

    public interface ITally { }
}