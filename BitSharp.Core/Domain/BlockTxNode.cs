using BitSharp.Common;
using System;
using System.Collections.Immutable;

namespace BitSharp.Core.Domain
{
    public class BlockTxNode : IMerkleTreeNode<BlockTxNode>
    {
        public BlockTxNode(int index, int depth, UInt256 hash, bool pruned, ImmutableArray<byte>? txBytes)
        {
            Hash = hash;
            Index = index;
            Depth = depth;
            Pruned = pruned;
            EncodedTx = txBytes != null ? new EncodedTx(hash, txBytes.Value) : null;
        }

        public BlockTxNode(int index, int depth, UInt256 hash, bool pruned, EncodedTx encodedTx)
        {
            Hash = hash;
            Index = index;
            Depth = depth;
            Pruned = pruned;
            EncodedTx = encodedTx;
        }

        public UInt256 Hash { get; }

        public int Index { get; }

        public int Depth { get; }

        public bool Pruned { get; }

        public EncodedTx EncodedTx { get; }

        public ImmutableArray<byte> TxBytes => EncodedTx.TxBytes;

        public BlockTx ToBlockTx()
        {
            // verify this is a valid BlockTx (cannot be pruned)
            if (Pruned || Depth > 0)
                throw new InvalidOperationException($"Cannot convert a BlockTxNode to a BlockTx with pruned: {Pruned} and depth: {Depth}");

            return new BlockTx(Index, EncodedTx);
        }

        public BlockTxNode AsPruned()
        {
            return new BlockTxNode(Index, Depth, Hash, pruned: true, encodedTx: null);
        }

        public BlockTxNode AsPruned(int index, int depth, UInt256 hash)
        {
            return new BlockTxNode(index, depth, hash, pruned: true, encodedTx: null);
        }

        //TODO only exists for tests
        public override bool Equals(object obj)
        {
            if (!(obj is BlockTxNode))
                return false;

            var other = (BlockTxNode)obj;
            return other.Index == this.Index && other.Depth == this.Depth && other.Hash == this.Hash && other.Pruned == this.Pruned;
        }
    }
}
