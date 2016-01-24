using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Linq;

namespace BitSharp.Network.Test
{
    [TestClass]
    public partial class NetworkEncoderTest
    {
        [TestMethod]
        public void TestWireEncodeAddressPayload()
        {
            var actual = NetworkEncoder.EncodeAddressPayload(ADDRESS_PAYLOAD_1);
            CollectionAssert.AreEqual(ADDRESS_PAYLOAD_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireDecodeAddressPayload()
        {
            var actual = NetworkEncoder.EncodeAddressPayload(NetworkEncoder.DecodeAddressPayload(ADDRESS_PAYLOAD_1_BYTES.ToArray()));
            CollectionAssert.AreEqual(ADDRESS_PAYLOAD_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireEncodeAlertPayload()
        {
            var actual = NetworkEncoder.EncodeAlertPayload(ALERT_PAYLOAD_1);
            CollectionAssert.AreEqual(ALERT_PAYLOAD_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireDecodeAlertPayload()
        {
            var actual = NetworkEncoder.EncodeAlertPayload(NetworkEncoder.DecodeAlertPayload(ALERT_PAYLOAD_1_BYTES.ToArray()));
            CollectionAssert.AreEqual(ALERT_PAYLOAD_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireEncodeGetBlocksPayload()
        {
            var actual = NetworkEncoder.EncodeGetBlocksPayload(GET_BLOCKS_PAYLOAD_1);
            CollectionAssert.AreEqual(GET_BLOCKS_PAYLOAD_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireDecodeGetBlocksPayload()
        {
            var actual = NetworkEncoder.EncodeGetBlocksPayload(NetworkEncoder.DecodeGetBlocksPayload(GET_BLOCKS_PAYLOAD_1_BYTES.ToArray()));
            CollectionAssert.AreEqual(GET_BLOCKS_PAYLOAD_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireEncodeInventoryPayload()
        {
            var actual = NetworkEncoder.EncodeInventoryPayload(INVENTORY_PAYLOAD_1);
            CollectionAssert.AreEqual(INVENTORY_PAYLOAD_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireDecodeInventoryPayload()
        {
            var actual = NetworkEncoder.EncodeInventoryPayload(NetworkEncoder.DecodeInventoryPayload(INVENTORY_PAYLOAD_1_BYTES.ToArray()));
            CollectionAssert.AreEqual(INVENTORY_PAYLOAD_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireEncodeInventoryVector()
        {
            var actual = NetworkEncoder.EncodeInventoryVector(INVENTORY_VECTOR_1);
            CollectionAssert.AreEqual(INVENTORY_VECTOR_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireDecodeInventoryVector()
        {
            var actual = NetworkEncoder.EncodeInventoryVector(NetworkEncoder.DecodeInventoryVector(INVENTORY_VECTOR_1_BYTES.ToArray()));
            CollectionAssert.AreEqual(INVENTORY_VECTOR_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireEncodeMessage()
        {
            var actual = NetworkEncoder.EncodeMessage(MESSAGE_1);
            CollectionAssert.AreEqual(MESSAGE_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireDecodeMessage()
        {
            var actual = NetworkEncoder.EncodeMessage(NetworkEncoder.DecodeMessage(MESSAGE_1_BYTES.ToArray()));
            CollectionAssert.AreEqual(MESSAGE_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireEncodeNetworkAddress()
        {
            var actual = NetworkEncoder.EncodeNetworkAddress(NETWORK_ADDRESS_1);
            CollectionAssert.AreEqual(NETWORK_ADDRESS_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireDecodeNetworkAddress()
        {
            var actual = NetworkEncoder.EncodeNetworkAddress(NetworkEncoder.DecodeNetworkAddress(NETWORK_ADDRESS_1_BYTES.ToArray()));
            CollectionAssert.AreEqual(NETWORK_ADDRESS_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireEncodeNetworkAddressWithTime()
        {
            var actual = NetworkEncoder.EncodeNetworkAddressWithTime(NETWORK_ADDRESS_WITH_TIME_1);
            CollectionAssert.AreEqual(NETWORK_ADDRESS_WITH_TIME_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireDecodeNetworkAddressWithTime()
        {
            var actual = NetworkEncoder.EncodeNetworkAddressWithTime(NetworkEncoder.DecodeNetworkAddressWithTime(NETWORK_ADDRESS_WITH_TIME_1_BYTES.ToArray()));
            CollectionAssert.AreEqual(NETWORK_ADDRESS_WITH_TIME_1_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireEncodeVersionPayloadWithoutRelay()
        {
            var actual = NetworkEncoder.EncodeVersionPayload(VERSION_PAYLOAD_1_NO_RELAY, withRelay: false);
            CollectionAssert.AreEqual(VERSION_PAYLOAD_1_NO_RELAY_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireEncodeVersionPayloadWithRelay()
        {
            var actual = NetworkEncoder.EncodeVersionPayload(VERSION_PAYLOAD_2_RELAY, withRelay: true);
            CollectionAssert.AreEqual(VERSION_PAYLOAD_2_RELAY_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireDecodeVersionPayloadWithoutRelay()
        {
            var actual = NetworkEncoder.EncodeVersionPayload(NetworkEncoder.DecodeVersionPayload(VERSION_PAYLOAD_1_NO_RELAY_BYTES.ToArray(), VERSION_PAYLOAD_1_NO_RELAY_BYTES.Length), withRelay: false);
            CollectionAssert.AreEqual(VERSION_PAYLOAD_1_NO_RELAY_BYTES.ToList(), actual.ToList());
        }

        [TestMethod]
        public void TestWireDecodeVersionPayloadWithRelay()
        {
            var actual = NetworkEncoder.EncodeVersionPayload(NetworkEncoder.DecodeVersionPayload(VERSION_PAYLOAD_2_RELAY_BYTES.ToArray(), VERSION_PAYLOAD_2_RELAY_BYTES.Length), withRelay: true);
            CollectionAssert.AreEqual(VERSION_PAYLOAD_2_RELAY_BYTES.ToList(), actual.ToList());
        }
    }
}
