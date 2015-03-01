using BitSharp.Common;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage.Memory;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;

namespace BitSharp.Core.Test
{
    [TestClass]
    public class MerkleTreeTest
    {
        [TestMethod]
        public void TestPruneMerkleTreeNodes()
        {
            var node1 = new MerkleTreeNode(index: 0, depth: 0, hash: 1, pruned: false);
            var node2 = new MerkleTreeNode(index: 1, depth: 0, hash: 2, pruned: false);
            var node3 = new MerkleTreeNode(index: 2, depth: 0, hash: 3, pruned: false);
            var node4 = new MerkleTreeNode(index: 3, depth: 0, hash: 4, pruned: false);
            var node5 = new MerkleTreeNode(index: 4, depth: 0, hash: 5, pruned: false);
            var node6 = new MerkleTreeNode(index: 5, depth: 0, hash: 6, pruned: false);
            var node7 = new MerkleTreeNode(index: 6, depth: 0, hash: 7, pruned: false);

            var depth1Node1 = node1.AsPruned().PairWith(node2.AsPruned());
            var depth1Node2 = node3.AsPruned().PairWith(node4.AsPruned());
            var depth1Node3 = node5.AsPruned().PairWith(node6.AsPruned());
            var depth1Node4 = node7.AsPruned().PairWithSelf();

            var depth2Node1 = depth1Node1.PairWith(depth1Node2);
            var depth2Node2 = depth1Node3.PairWith(depth1Node4);

            var merkleRoot = depth2Node1.PairWith(depth2Node2);

            var nodes = new List<MerkleTreeNode> { node1, node2, node3, node4, node5, node6, node7 };

            var cursor = new MemoryMerkleTreePruningCursor(nodes);

            //////////////////////////////////////////////////

            var expectedNodes1 = nodes;
            var actualNodes1 = cursor.ReadNodes().ToList();
            CollectionAssert.AreEqual(expectedNodes1, actualNodes1);

            //////////////////////////////////////////////////

            MerkleTree.PruneNode(cursor, 2);

            var expectedNodes2 = new List<MerkleTreeNode> { node1, node2, node3.AsPruned(), node4, node5, node6, node7 };
            var actualNodes2 = cursor.ReadNodes().ToList();
            CollectionAssert.AreEqual(expectedNodes2, actualNodes2);

            //////////////////////////////////////////////////

            MerkleTree.PruneNode(cursor, 0);

            var expectedNodes3 = new List<MerkleTreeNode> { node1.AsPruned(), node2, node3.AsPruned(), node4, node5, node6, node7 };
            var actualNodes3 = cursor.ReadNodes().ToList();
            CollectionAssert.AreEqual(expectedNodes3, actualNodes3);

            //////////////////////////////////////////////////

            MerkleTree.PruneNode(cursor, 1);

            var expectedNodes4 = new List<MerkleTreeNode> { depth1Node1, node3.AsPruned(), node4, node5, node6, node7 };
            var actualNodes4 = cursor.ReadNodes().ToList();
            CollectionAssert.AreEqual(expectedNodes4, actualNodes4);

            //////////////////////////////////////////////////

            MerkleTree.PruneNode(cursor, 3);

            var expectedNodes5 = new List<MerkleTreeNode> { depth2Node1, node5, node6, node7 };
            var actualNodes5 = cursor.ReadNodes().ToList();
            CollectionAssert.AreEqual(expectedNodes5, actualNodes5);

            //////////////////////////////////////////////////

            MerkleTree.PruneNode(cursor, 5);

            var expectedNodes6 = new List<MerkleTreeNode> { depth2Node1, node5, node6.AsPruned(), node7 };
            var actualNodes6 = cursor.ReadNodes().ToList();
            CollectionAssert.AreEqual(expectedNodes6, actualNodes6);

            //////////////////////////////////////////////////

            MerkleTree.PruneNode(cursor, 6);

            var expectedNodes8 = new List<MerkleTreeNode> { depth2Node1, node5, node6.AsPruned(), depth1Node4 };
            var actualNodes8 = cursor.ReadNodes().ToList();
            CollectionAssert.AreEqual(expectedNodes8, actualNodes8);

            //////////////////////////////////////////////////

            MerkleTree.PruneNode(cursor, 4);

            var expectedNodes9 = new List<MerkleTreeNode> { merkleRoot };
            var actualNodes9 = cursor.ReadNodes().ToList();
            CollectionAssert.AreEqual(expectedNodes9, actualNodes9);
        }

        [TestMethod]
        public void TestReadMerkleTreeNodes()
        {
            var node1 = new MerkleTreeNode(index: 0, depth: 0, hash: 1, pruned: false);
            var node2 = new MerkleTreeNode(index: 1, depth: 0, hash: 2, pruned: false);
            var node3 = new MerkleTreeNode(index: 2, depth: 0, hash: 3, pruned: false);
            var node4 = new MerkleTreeNode(index: 3, depth: 0, hash: 4, pruned: false);

            var depth1Hash1 = MerkleTree.PairHashes(node1.Hash, node2.Hash);
            var depth1Hash2 = MerkleTree.PairHashes(node3.Hash, node4.Hash);
            var merkleRoot = MerkleTree.PairHashes(depth1Hash1, depth1Hash2);

            var nodes = new List<MerkleTreeNode> { node1, node2, node3, node4 };

            var actualNodes = MerkleTree.ReadMerkleTreeNodes(merkleRoot, nodes).ToList();

            CollectionAssert.AreEqual(nodes, actualNodes);
        }

        [TestMethod]
        public void TestReadMerkleTreeNodesOddPower()
        {
            var depth0Hash1 = (UInt256)1;
            var depth0Hash2 = (UInt256)2;
            var depth0Hash3 = (UInt256)3;

            var depth1Hash1 = MerkleTree.PairHashes(depth0Hash1, depth0Hash2);
            var depth1Hash2 = MerkleTree.PairHashes(depth0Hash3, depth0Hash3);

            var merkleRoot = MerkleTree.PairHashes(depth1Hash1, depth1Hash2);

            var nodes = new List<MerkleTreeNode> { 
                new MerkleTreeNode(index: 0, depth: 0, hash: depth0Hash1, pruned: false),
                new MerkleTreeNode(index: 1, depth: 0, hash: depth0Hash2, pruned: false),
                new MerkleTreeNode(index: 2, depth: 0, hash: depth0Hash3, pruned: false),
            };

            var actualNodes = MerkleTree.ReadMerkleTreeNodes(merkleRoot, nodes).ToList();

            CollectionAssert.AreEqual(nodes, actualNodes);
        }

        [TestMethod]
        public void TestReadMerkleTreeNodesPruned()
        {
            var depth0Hash1 = (UInt256)1;
            var depth0Hash2 = (UInt256)2;
            var depth0Hash3 = (UInt256)3;
            var depth0Hash4 = (UInt256)4;
            var depth0Hash5 = (UInt256)5;
            var depth0Hash6 = (UInt256)6;
            var depth0Hash7 = (UInt256)7;
            var depth0Hash8 = (UInt256)8;
            var depth0Hash9 = (UInt256)9;
            var depth0Hash10 = (UInt256)10;
            var depth0Hash11 = (UInt256)11;
            var depth0Hash12 = (UInt256)12;
            var depth0Hash13 = (UInt256)13;
            var depth0Hash14 = (UInt256)14;
            var depth0Hash15 = (UInt256)15;

            var depth1Hash1 = MerkleTree.PairHashes(depth0Hash1, depth0Hash2);
            var depth1Hash2 = MerkleTree.PairHashes(depth0Hash3, depth0Hash4);
            var depth1Hash3 = MerkleTree.PairHashes(depth0Hash5, depth0Hash6);
            var depth1Hash4 = MerkleTree.PairHashes(depth0Hash7, depth0Hash8);
            var depth1Hash5 = MerkleTree.PairHashes(depth0Hash9, depth0Hash10);
            var depth1Hash6 = MerkleTree.PairHashes(depth0Hash11, depth0Hash12);
            var depth1Hash7 = MerkleTree.PairHashes(depth0Hash13, depth0Hash14);
            var depth1Hash8 = MerkleTree.PairHashes(depth0Hash15, depth0Hash15);

            var depth2Hash1 = MerkleTree.PairHashes(depth1Hash1, depth1Hash2);
            var depth2Hash2 = MerkleTree.PairHashes(depth1Hash3, depth1Hash4);
            var depth2Hash3 = MerkleTree.PairHashes(depth1Hash5, depth1Hash6);
            var depth2Hash4 = MerkleTree.PairHashes(depth1Hash7, depth1Hash8);

            var depth3Hash1 = MerkleTree.PairHashes(depth2Hash1, depth2Hash2);
            var depth3Hash2 = MerkleTree.PairHashes(depth2Hash3, depth2Hash4);

            var merkleRoot = MerkleTree.PairHashes(depth3Hash1, depth3Hash2);

            var nodes = new List<MerkleTreeNode> { 
                new MerkleTreeNode(index: 0, depth: 0, hash: depth0Hash1, pruned: false),
                new MerkleTreeNode(index: 1, depth: 0, hash: depth0Hash2, pruned: false),
                new MerkleTreeNode(index: 2, depth: 0, hash: depth0Hash3, pruned: false),
                new MerkleTreeNode(index: 3, depth: 0, hash: depth0Hash4, pruned: false),
                        new MerkleTreeNode(index: 4, depth: 2, hash: depth2Hash2, pruned: true),
                new MerkleTreeNode(index: 8, depth: 0, hash: depth0Hash9, pruned: false),
                new MerkleTreeNode(index: 9, depth: 0, hash: depth0Hash10, pruned: false),
                    new MerkleTreeNode(index: 10, depth: 1, hash: depth1Hash6, pruned: true),
                new MerkleTreeNode(index: 12, depth: 0, hash: depth0Hash13, pruned: false),
                new MerkleTreeNode(index: 13, depth: 0, hash: depth0Hash14, pruned: false),
                    new MerkleTreeNode(index: 14, depth: 1, hash: depth1Hash8, pruned: true),
            };

            var actualNodes = MerkleTree.ReadMerkleTreeNodes(merkleRoot, nodes).ToList();

            CollectionAssert.AreEqual(nodes, actualNodes);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void TestReadMerkleTreeNodesBadDepth()
        {
            var depth0Hash1 = (UInt256)1;
            var depth0Hash2 = (UInt256)2;
            var depth0Hash3 = (UInt256)3;

            var depth1Hash1 = MerkleTree.PairHashes(depth0Hash1, depth0Hash2);
            var depth1Hash2 = MerkleTree.PairHashes(depth0Hash3, depth0Hash3);

            var merkleRoot = MerkleTree.PairHashes(depth1Hash1, depth1Hash2);

            var nodes = new List<MerkleTreeNode> { 
                new MerkleTreeNode(index: 0, depth: 0, hash: depth0Hash1, pruned: false),
                new MerkleTreeNode(index: 1, depth: 0, hash: depth0Hash2, pruned: false),
                new MerkleTreeNode(index: 2, depth: 1, hash: depth0Hash3, pruned: true),
            };

            var actualNodes = MerkleTree.ReadMerkleTreeNodes(merkleRoot, nodes).ToList();

            CollectionAssert.AreEqual(nodes, actualNodes);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void TestReadMerkleTreeNodesBadIndex()
        {
            var depth0Hash1 = (UInt256)1;
            var depth0Hash2 = (UInt256)2;
            var depth0Hash3 = (UInt256)3;

            var depth1Hash1 = MerkleTree.PairHashes(depth0Hash1, depth0Hash2);
            var depth1Hash2 = MerkleTree.PairHashes(depth0Hash3, depth0Hash3);

            var merkleRoot = MerkleTree.PairHashes(depth1Hash1, depth1Hash2);

            var nodes = new List<MerkleTreeNode> { 
                new MerkleTreeNode(index: 0, depth: 0, hash: depth0Hash1, pruned: false),
                new MerkleTreeNode(index: 1, depth: 0, hash: depth0Hash2, pruned: false),
                new MerkleTreeNode(index: 3, depth: 0, hash: depth0Hash3, pruned: false),
            };

            var actualNodes = MerkleTree.ReadMerkleTreeNodes(merkleRoot, nodes).ToList();

            CollectionAssert.AreEqual(nodes, actualNodes);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestMerkleTreeNodeBadIndex()
        {
            new MerkleTreeNode(index: 3, depth: 1, hash: 0, pruned: false);
        }

        [TestMethod]
        public void TestCalculateMerkleRoot()
        {
            var node1 = new MerkleTreeNode(index: 0, depth: 0, hash: 1, pruned: false);
            var node2 = new MerkleTreeNode(index: 1, depth: 0, hash: 2, pruned: false);
            var node3 = new MerkleTreeNode(index: 2, depth: 0, hash: 3, pruned: false);

            var depth1Hash1 = MerkleTree.PairHashes(node1.Hash, node2.Hash);
            var depth1Hash2 = MerkleTree.PairHashes(node3.Hash, node3.Hash);
            
            var expectedMerkleRoot = MerkleTree.PairHashes(depth1Hash1, depth1Hash2);

            var hashes = new List<UInt256> { node1.Hash, node2.Hash, node3.Hash };
            var actualMerkleRoot = MerkleTree.CalculateMerkleRoot(hashes); ;

            Assert.AreEqual(expectedMerkleRoot, actualMerkleRoot);
        }
    }
}
