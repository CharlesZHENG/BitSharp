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

        void PreValidateBlock(Chain newChain);

        // executed serially, in order
        void TallyTransaction(Chain newChain, ValidatableTx validatableTx, ref object runningTally);

        // executed in parallel, any order
        void ValidateTransaction(Chain newChain, ValidatableTx validatableTx);

        void ValidationTransactionScript(Chain newChain, BlockTx tx, TxInput txInput, int txInputIndex, PrevTxOutput prevTxOutput);

        void PostValidateBlock(Chain newChain, object finalTally);
    }

    public interface ITally { }
}