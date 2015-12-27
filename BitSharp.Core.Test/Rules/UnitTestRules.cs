using BitSharp.Common;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using System;
using System.Collections.Immutable;

namespace BitSharp.Core.Test.Rules
{
    public class UnitTestRules : MainnetRules
    {
        public static readonly UInt256 Target0 = UInt256.ParseHex("FFFFFF0000000000000000000000000000000000000000000000000000000000");
        public static readonly UInt256 Target1 = UInt256.ParseHex("0FFFFFF000000000000000000000000000000000000000000000000000000000");
        public static readonly UInt256 Target2 = UInt256.ParseHex("00FFFFFF00000000000000000000000000000000000000000000000000000000");
        public static readonly UInt256 Target3 = UInt256.ParseHex("000FFFFFF0000000000000000000000000000000000000000000000000000000");
        public static readonly UInt256 Target4 = UInt256.ParseHex("0000FFFFFF000000000000000000000000000000000000000000000000000000");

        private UInt256 _highestTarget;
        private Block _genesisBlock;
        private ChainedHeader _genesisChainedHeader;

        public UnitTestRules()
        {
            this._highestTarget = Target0;
        }

        public override UInt256 HighestTarget => this._highestTarget;

        public override Block GenesisBlock => this._genesisBlock;

        public override ChainedHeader GenesisChainedHeader => this._genesisChainedHeader;

        public Action<Chain, ChainedHeader> PreValidateBlockAction { get; set; }

        public override void PreValidateBlock(Chain chain, ChainedHeader chainedHeader)
        {
            if (PreValidateBlockAction == null)
                base.PreValidateBlock(chain, chainedHeader);
            else
                PreValidateBlockAction(chain, chainedHeader);
        }

        public Action<Chain, ChainedHeader, Transaction, ulong, ulong> PostValidateBlockAction { get; set; }

        public override void PostValidateBlock(Chain chain, ChainedHeader chainedHeader, Transaction coinbaseTx, ulong totalTxInputValue, ulong totalTxOutputValue)
        {
            if (PreValidateBlockAction == null)
                base.PostValidateBlock(chain, chainedHeader, coinbaseTx, totalTxInputValue, totalTxOutputValue);
            else
                PostValidateBlockAction(chain, chainedHeader, coinbaseTx, totalTxInputValue, totalTxOutputValue);
        }

        public Action<ChainedHeader, ValidatableTx> ValidateTransactionAction { get; set; }

        public override void ValidateTransaction(ChainedHeader chainedHeader, ValidatableTx validatableTx)
        {
            if (ValidateTransactionAction == null)
                base.ValidateTransaction(chainedHeader, validatableTx);
            else
                ValidateTransactionAction(chainedHeader, validatableTx);
        }

        public Action<ChainedHeader, BlockTx, TxInput, int, PrevTxOutput> ValidationTransactionScriptAction { get; set; }

        public override void ValidationTransactionScript(ChainedHeader chainedHeader, BlockTx tx, TxInput txInput, int txInputIndex, PrevTxOutput prevTxOutput)
        {
            if (ValidationTransactionScriptAction == null)
                base.ValidationTransactionScript(chainedHeader, tx, txInput, txInputIndex, prevTxOutput);
            else
                ValidationTransactionScriptAction(chainedHeader, tx, txInput, txInputIndex, prevTxOutput);
        }

        public void SetGenesisBlock(Block genesisBlock)
        {
            this._genesisBlock = genesisBlock;
            this._genesisChainedHeader = ChainedHeader.CreateForGenesisBlock(this._genesisBlock.Header);
        }

        public void SetHighestTarget(UInt256 highestTarget)
        {
            this._highestTarget = highestTarget;
        }
    }
}
