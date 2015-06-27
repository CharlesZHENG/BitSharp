using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage;
using NLog;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BitSharp.Core.Builders
{
    internal interface IDeferredChainStateCursor : IChainStateCursor
    {
        void WarmUnspentTx(UInt256 txHash);

        void ApplyChangesToParent(IChainStateCursor parent);
    }
}
