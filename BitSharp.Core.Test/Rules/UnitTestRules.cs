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

        public Action<Chain> PreValidateBlockAction { get; set; }

        public Func<Chain, ValidatableTx, object, object> TallyTransactionFunc { get; set; }

        public Action<Chain, ValidatableTx> ValidateTransactionAction { get; set; }

        public Action<Chain, BlockTx, TxInput, int, PrevTxOutput> ValidationTransactionScriptAction { get; set; }

        public Action<Chain, object> PostValidateBlockAction { get; set; }

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

        public void PreValidateBlock(Chain newChain)
        {
            if (PreValidateBlockAction == null)
                coreRules.PreValidateBlock(newChain);
            else
                PreValidateBlockAction(newChain);
        }

        public void TallyTransaction(Chain newChain, ValidatableTx validatableTx, ref object runningTally)
        {
            if (TallyTransactionFunc == null)
                coreRules.TallyTransaction(newChain, validatableTx, ref runningTally);
            else
            {
                runningTally = TallyTransactionFunc(newChain, validatableTx, runningTally);
            }
        }

        public void ValidateTransaction(Chain newChain, ValidatableTx validatableTx)
        {
            if (ValidateTransactionAction == null)
                coreRules.ValidateTransaction(newChain, validatableTx);
            else
                ValidateTransactionAction(newChain, validatableTx);
        }

        public void ValidationTransactionScript(Chain newChain, BlockTx tx, TxInput txInput, int txInputIndex, PrevTxOutput prevTxOutput)
        {
            if (ValidationTransactionScriptAction == null)
                coreRules.ValidationTransactionScript(newChain, tx, txInput, txInputIndex, prevTxOutput);
            else
                ValidationTransactionScriptAction(newChain, tx, txInput, txInputIndex, prevTxOutput);
        }

        public void PostValidateBlock(Chain newChain, object finalTally)
        {
            if (PreValidateBlockAction == null)
                coreRules.PostValidateBlock(newChain, finalTally);
            else
                PostValidateBlockAction(newChain, finalTally);
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

        public int MajorityWindow => mainnetParams.MajorityWindow;

        public int MajorityEnforceBlockUpgrade => mainnetParams.MajorityEnforceBlockUpgrade;

        public int MajorityRejectBlockOutdated => mainnetParams.MajorityRejectBlockOutdated;

        public void SetGenesisBlock(Block genesisBlock)
        {
            GenesisBlock = genesisBlock;
            GenesisChainedHeader = ChainedHeader.CreateForGenesisBlock(genesisBlock.Header);
        }
    }
}
