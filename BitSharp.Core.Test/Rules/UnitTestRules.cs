using BitSharp.Common;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using System;
using System.Collections.Immutable;

namespace BitSharp.Core.Test.Rules
{
    public class UnitTestRules : ICoreRules
    {
        private readonly CoreRules coreRules;

        public UnitTestRules()
        {
            ChainParams = new UnitTestParams { HighestTarget = UnitTestParams.Target0 };
            coreRules = new CoreRules(ChainParams);
        }

        public UnitTestParams ChainParams { get; }
        IChainParams ICoreRules.ChainParams => ChainParams;

        public Action<Chain, ChainedHeader> PreValidateBlockAction { get; set; }

        public Func<ChainedHeader, ValidatableTx, object, object> TallyTransactionFunc { get; set; }

        public Action<ChainedHeader, ValidatableTx> ValidateTransactionAction { get; set; }

        public Action<ChainedHeader, BlockTx, TxInput, int, PrevTxOutput> ValidationTransactionScriptAction { get; set; }

        public Action<Chain, ChainedHeader, object> PostValidateBlockAction { get; set; }

        public bool IgnoreScripts
        {
            get { return coreRules.IgnoreScripts; }
            set { coreRules.IgnoreScripts = value; }
        }

        public bool IgnoreSignatures
        {
            get { return coreRules.IgnoreSignatures; }
            set { coreRules.IgnoreSignatures = value; }
        }

        public bool IgnoreScriptErrors
        {
            get { return coreRules.IgnoreScriptErrors; }
            set { coreRules.IgnoreScriptErrors = value; }
        }

        public void PreValidateBlock(Chain chain, ChainedHeader chainedHeader)
        {
            if (PreValidateBlockAction == null)
                coreRules.PreValidateBlock(chain, chainedHeader);
            else
                PreValidateBlockAction(chain, chainedHeader);
        }

        public void TallyTransaction(ChainedHeader chainedHeader, ValidatableTx validatableTx, ref object runningTally)
        {
            if (TallyTransactionFunc == null)
                coreRules.TallyTransaction(chainedHeader, validatableTx, ref runningTally);
            else
            {
                runningTally = TallyTransactionFunc(chainedHeader, validatableTx, runningTally);
            }
        }

        public void ValidateTransaction(ChainedHeader chainedHeader, ValidatableTx validatableTx)
        {
            if (ValidateTransactionAction == null)
                coreRules.ValidateTransaction(chainedHeader, validatableTx);
            else
                ValidateTransactionAction(chainedHeader, validatableTx);
        }

        public void ValidationTransactionScript(ChainedHeader chainedHeader, BlockTx tx, TxInput txInput, int txInputIndex, PrevTxOutput prevTxOutput)
        {
            if (ValidationTransactionScriptAction == null)
                coreRules.ValidationTransactionScript(chainedHeader, tx, txInput, txInputIndex, prevTxOutput);
            else
                ValidationTransactionScriptAction(chainedHeader, tx, txInput, txInputIndex, prevTxOutput);
        }

        public void PostValidateBlock(Chain chain, ChainedHeader chainedHeader, object finalTally)
        {
            if (PreValidateBlockAction == null)
                coreRules.PostValidateBlock(chain, chainedHeader, finalTally);
            else
                PostValidateBlockAction(chain, chainedHeader, finalTally);
        }
    }

    public class UnitTestParams : IChainParams
    {
        public static readonly UInt256 Target0 = UInt256.ParseHex("FFFFFF0000000000000000000000000000000000000000000000000000000000");
        public static readonly UInt256 Target1 = UInt256.ParseHex("0FFFFFF000000000000000000000000000000000000000000000000000000000");
        public static readonly UInt256 Target2 = UInt256.ParseHex("00FFFFFF00000000000000000000000000000000000000000000000000000000");
        public static readonly UInt256 Target3 = UInt256.ParseHex("000FFFFFF0000000000000000000000000000000000000000000000000000000");
        public static readonly UInt256 Target4 = UInt256.ParseHex("0000FFFFFF000000000000000000000000000000000000000000000000000000");

        private readonly MainnetParams mainnetParams = new MainnetParams();

        public UInt256 GenesisHash => GenesisBlock?.Hash;

        public Block GenesisBlock { get; set; }

        public ChainedHeader GenesisChainedHeader { get; set; }

        public UInt256 HighestTarget { get; set; }

        public int DifficultyInterval => mainnetParams.DifficultyInterval;

        public int DifficultyTargetTimespan => mainnetParams.DifficultyTargetTimespan;

        public bool AllowMininimumDifficultyBlocks => mainnetParams.AllowMininimumDifficultyBlocks;

        public bool PowNoRetargeting => mainnetParams.PowNoRetargeting;

        public int PowTargetSpacing => mainnetParams.PowTargetSpacing;

        public void SetGenesisBlock(Block genesisBlock)
        {
            GenesisBlock = genesisBlock;
            GenesisChainedHeader = ChainedHeader.CreateForGenesisBlock(genesisBlock.Header);
        }
    }
}
