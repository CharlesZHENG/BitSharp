using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Linq;

namespace BitSharp.Node.Test
{
    [TestClass]
    public partial class NodeEncoderTest
    {
        [TestMethod]
        public void TestWireEncodeAddressPayload()
        {
            var actual = NodeEncoder.EncodeAddressPayload(ADDRESS_PAYLOAD_1);
            CollectionAssert.AreEqual(ADDRESS_PAYLOAD_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireDecodeAddressPayload()
        {
            var actual = NodeEncoder.EncodeAddressPayload(NodeEncoder.DecodeAddressPayload(ADDRESS_PAYLOAD_1_BYTES.ToArray()));
            CollectionAssert.AreEqual(ADDRESS_PAYLOAD_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireEncodeAlertPayload()
        {
            var actual = NodeEncoder.EncodeAlertPayload(ALERT_PAYLOAD_1);
            CollectionAssert.AreEqual(ALERT_PAYLOAD_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireDecodeAlertPayload()
        {
            var actual = NodeEncoder.EncodeAlertPayload(NodeEncoder.DecodeAlertPayload(ALERT_PAYLOAD_1_BYTES.ToArray()));
            CollectionAssert.AreEqual(ALERT_PAYLOAD_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireEncodeGetBlocksPayload()
        {
            var actual = NodeEncoder.EncodeGetBlocksPayload(GET_BLOCKS_PAYLOAD_1);
            CollectionAssert.AreEqual(GET_BLOCKS_PAYLOAD_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireDecodeGetBlocksPayload()
        {
            var actual = NodeEncoder.EncodeGetBlocksPayload(NodeEncoder.DecodeGetBlocksPayload(GET_BLOCKS_PAYLOAD_1_BYTES.ToArray()));
            CollectionAssert.AreEqual(GET_BLOCKS_PAYLOAD_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireEncodeInventoryPayload()
        {
            var actual = NodeEncoder.EncodeInventoryPayload(INVENTORY_PAYLOAD_1);
            CollectionAssert.AreEqual(INVENTORY_PAYLOAD_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireDecodeInventoryPayload()
        {
            var actual = NodeEncoder.EncodeInventoryPayload(NodeEncoder.DecodeInventoryPayload(INVENTORY_PAYLOAD_1_BYTES.ToArray()));
            CollectionAssert.AreEqual(INVENTORY_PAYLOAD_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireEncodeInventoryVector()
        {
            var actual = NodeEncoder.EncodeInventoryVector(INVENTORY_VECTOR_1);
            CollectionAssert.AreEqual(INVENTORY_VECTOR_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireDecodeInventoryVector()
        {
            var actual = NodeEncoder.EncodeInventoryVector(NodeEncoder.DecodeInventoryVector(INVENTORY_VECTOR_1_BYTES.ToArray()));
            CollectionAssert.AreEqual(INVENTORY_VECTOR_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireEncodeMessage()
        {
            var actual = NodeEncoder.EncodeMessage(MESSAGE_1);
            CollectionAssert.AreEqual(MESSAGE_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireDecodeMessage()
        {
            var actual = NodeEncoder.EncodeMessage(NodeEncoder.DecodeMessage(MESSAGE_1_BYTES.ToArray()));
            CollectionAssert.AreEqual(MESSAGE_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireEncodeNetworkAddress()
        {
            var actual = NodeEncoder.EncodeNetworkAddress(NETWORK_ADDRESS_1);
            CollectionAssert.AreEqual(NETWORK_ADDRESS_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireDecodeNetworkAddress()
        {
            var actual = NodeEncoder.EncodeNetworkAddress(NodeEncoder.DecodeNetworkAddress(NETWORK_ADDRESS_1_BYTES.ToArray()));
            CollectionAssert.AreEqual(NETWORK_ADDRESS_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireEncodeNetworkAddressWithTime()
        {
            var actual = NodeEncoder.EncodeNetworkAddressWithTime(NETWORK_ADDRESS_WITH_TIME_1);
            CollectionAssert.AreEqual(NETWORK_ADDRESS_WITH_TIME_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireDecodeNetworkAddressWithTime()
        {
            var actual = NodeEncoder.EncodeNetworkAddressWithTime(NodeEncoder.DecodeNetworkAddressWithTime(NETWORK_ADDRESS_WITH_TIME_1_BYTES.ToArray()));
            CollectionAssert.AreEqual(NETWORK_ADDRESS_WITH_TIME_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireEncodeVersionPayloadWithoutRelay()
        {
            var actual = NodeEncoder.EncodeVersionPayload(VERSION_PAYLOAD_1_NO_RELAY, withRelay: false);
            CollectionAssert.AreEqual(VERSION_PAYLOAD_1_NO_RELAY_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireEncodeVersionPayloadWithRelay()
        {
            var actual = NodeEncoder.EncodeVersionPayload(VERSION_PAYLOAD_2_RELAY, withRelay: true);
            CollectionAssert.AreEqual(VERSION_PAYLOAD_2_RELAY_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireDecodeVersionPayloadWithoutRelay()
        {
            var actual = NodeEncoder.EncodeVersionPayload(NodeEncoder.DecodeVersionPayload(VERSION_PAYLOAD_1_NO_RELAY_BYTES.ToArray(), VERSION_PAYLOAD_1_NO_RELAY_BYTES.Length), withRelay: false);
            CollectionAssert.AreEqual(VERSION_PAYLOAD_1_NO_RELAY_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireDecodeVersionPayloadWithRelay()
        {
            var actual = NodeEncoder.EncodeVersionPayload(NodeEncoder.DecodeVersionPayload(VERSION_PAYLOAD_2_RELAY_BYTES.ToArray(), VERSION_PAYLOAD_2_RELAY_BYTES.Length), withRelay: true);
            CollectionAssert.AreEqual(VERSION_PAYLOAD_2_RELAY_BYTES.ToList(), actual.ToList());
        }
    }
}
