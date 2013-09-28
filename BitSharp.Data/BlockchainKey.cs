﻿using BitSharp.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BitSharp.Data
{
    public class BlockchainKey
    {
        private readonly Guid _guid;
        private readonly UInt256 _rootBlockHash;

        private readonly int hashCode;

        public BlockchainKey(Guid guid, UInt256 rootBlockHash)
        {
            this._guid = guid;
            this._rootBlockHash = rootBlockHash;

            this.hashCode = guid.GetHashCode() ^ rootBlockHash.GetHashCode();
        }

        public Guid Guid { get { return this._guid; } }

        public UInt256 RootBlockHash { get { return this._rootBlockHash; } }

        public override bool Equals(object obj)
        {
            if (!(obj is BlockchainKey))
                return false;

            return (BlockchainKey)obj == this;
        }

        public override int GetHashCode()
        {
            return this.hashCode;
        }

        public static bool operator ==(BlockchainKey left, BlockchainKey right)
        {
            return object.ReferenceEquals(left, right) || (!object.ReferenceEquals(left, null) && !object.ReferenceEquals(right, null) && left.Guid == right.Guid && left.RootBlockHash == right.RootBlockHash);
        }

        public static bool operator !=(BlockchainKey left, BlockchainKey right)
        {
            return !(left == right);
        }
    }
}
