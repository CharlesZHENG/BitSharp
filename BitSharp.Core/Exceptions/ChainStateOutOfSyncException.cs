using BitSharp.Core.Domain;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace BitSharp.Core
{
    public class ChainStateOutOfSyncException : InvalidOperationException
    {
        public ChainStateOutOfSyncException(ChainedHeader expectedChainTip, ChainedHeader actualChainTip)
        {
            ExpectedChainTip = expectedChainTip;
            ActualChainTip = actualChainTip;
        }

        public ChainedHeader ExpectedChainTip { get; private set; }

        public ChainedHeader ActualChainTip { get; private set; }
    }
}
