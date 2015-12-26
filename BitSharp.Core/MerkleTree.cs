using BitSharp.Common;
using BitSharp.Core.Domain;
using System;
using System.Collections.Generic;
using System.Linq;

namespace BitSharp.Core
{
    //TODO organize and name properly
    public static class MerkleTree
    {
        public static void PruneNode<T>(IMerkleTreePruningCursor<T> cursor, int index)
            where T : IMerkleTreeNode<T>
        {
            if (!cursor.TryMoveToIndex(index))
                return;

            var node = cursor.ReadNode();

            if (node.Depth != 0)
                return;

            if (!node.Pruned)
            {
                node = node.AsPruned();
                cursor.WriteNode(node);
            }

            bool didWork;
            do
            {
                didWork = false;

                if (node.IsLeft())
                {
                    if (cursor.TryMoveRight())
                    {
                        var rightNode = cursor.ReadNode();
                        if (node.Pruned && rightNode.Pruned && node.Depth == rightNode.Depth)
                        {
                            var newNode = node.PairWith(rightNode);

                            cursor.DeleteNode();
                            //TODO cursor.MoveLeft();
                            cursor.WriteNode(newNode);

                            node = newNode;
                            didWork = true;
                        }
                    }
                    else
                    {
                        if (node.Index != 0 && node.Pruned)
                        {
                            var newNode = node.PairWithSelf();
                            //cursor.MoveLeft();
                            cursor.WriteNode(newNode);

                            node = newNode;
                            didWork = true;
                        }
                    }
                }
                else
                {
                    if (cursor.TryMoveLeft())
                    {
                        var leftNode = cursor.ReadNode();
                        if (node.Pruned && leftNode.Pruned && node.Depth == leftNode.Depth)
                        {
                            var newNode = leftNode.PairWith(node);
                            cursor.WriteNode(newNode);
                            cursor.MoveRight();
                            cursor.DeleteNode();
                            //TODO cursor.MoveLeft();

                            node = newNode;
                            didWork = true;
                        }
                    }
                }
            }
            while (didWork);
        }

        public static UInt256 CalculateMerkleRoot(IEnumerable<Transaction> transactions)
        {
            return CalculateMerkleRoot(transactions.Select(x => x.Hash));
        }

        public static UInt256 CalculateMerkleRoot(IEnumerable<EncodedTx> transactions)
        {
            return CalculateMerkleRoot(transactions.Select(x => x.Hash));
        }

        public static UInt256 CalculateMerkleRoot(IEnumerable<BlockTx> transactions)
        {
            return CalculateMerkleRoot(transactions.Select(x => x.Hash));
        }

        public static UInt256 CalculateMerkleRoot(IEnumerable<UInt256> hashes)
        {
            var merkleStream = new MerkleStream<MerkleTreeNode>();

            var index = 0;
            foreach (var hash in hashes)
            {
                var node = new MerkleTreeNode(index, 0, hash, true);
                merkleStream.AddNode(node);
                index++;
            }

            merkleStream.FinishPairing();

            return merkleStream.RootNode.Hash;
        }

        public static UInt256 CalculateMerkleRoot<T>(IEnumerable<T> merkleTreeNodes)
            where T : IMerkleTreeNode<T>
        {
            var merkleStream = new MerkleStream<T>();

            foreach (var node in merkleTreeNodes)
            {
                merkleStream.AddNode(node);
            }

            merkleStream.FinishPairing();

            return merkleStream.RootNode.Hash;
        }

        public static IEnumerable<T> ReadMerkleTreeNodes<T>(UInt256 merkleRoot, IEnumerable<T> merkleTreeNodes)
            where T : IMerkleTreeNode<T>
        {
            var merkleStream = new MerkleStream<T>();

            foreach (var node in merkleTreeNodes)
            {
                merkleStream.AddNode(node);
                yield return node;
            }

            merkleStream.FinishPairing();

            if (merkleStream.RootNode.Hash != merkleRoot)
            {
                throw new InvalidOperationException();
            }

            yield break;
        }

        public static UInt256 PairHashes(UInt256 left, UInt256 right)
        {
            var bytes = new byte[64];
            left.ToByteArray(bytes, 0);
            right.ToByteArray(bytes, 32);
            return new UInt256(SHA256Static.ComputeDoubleHash(bytes));
        }
    }
}
