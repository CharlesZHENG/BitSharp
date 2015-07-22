using BitSharp.Core.Domain;
using System;

namespace BitSharp.Core
{
    public class ChainStateOutOfSyncException : InvalidOperationException
    {
        public ChainStateOutOfSyncException(ChainedHeader expectedChainTip, ChainedHeader actualChainTip)
        {
            ExpectedChainTip = expectedChainTip;
            ActualChainTip = actualChainTip;
        }

        public ChainedHeader ExpectedChainTip { get; }

        public ChainedHeader ActualChainTip { get; }
    }
}
