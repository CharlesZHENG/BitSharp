﻿using BitSharp.Common;
using BitSharp.Data;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BitSharp.Storage
{
    public interface ITransactionStorage : IUnboundedStorage<UInt256, Transaction>
    {
    }
}
