using BitSharp.Common;
using BitSharp.Core.Domain;

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

        //TODO
        //void ValidateBlock(ChainedBlock chainedBlock, ChainStateBuilder chainStateBuilder);

        void ValidateTransaction(ChainedHeader chainedHeader, LoadedTx loadedTx);

        void ValidationTransactionScript(ChainedHeader chainedHeader, Transaction tx, int txIndex, TxInput txInput, int txInputIndex, TxOutput prevTxOutput);
    }
}
