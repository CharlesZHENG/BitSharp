using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.ExtensionMethods;
using BitSharp.Node.Domain;
using System;
using System.Collections.Immutable;
using System.IO;
using System.Linq;

namespace BitSharp.Node
{
    public static class NodeEncoder
    {
        public static AlertPayload DecodeAlertPayload(BinaryReader reader)
        {
            return new AlertPayload
            (
                Payload: reader.ReadVarString(),
                Signature: reader.ReadVarString()
            );
        }

        public static AlertPayload DecodeAlertPayload(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            using (var reader = new BinaryReader(stream))
            {
                return DecodeAlertPayload(reader);
            }
        }

        public static void EncodeAlertPayload(BinaryWriter writer, AlertPayload alertPayload)
        {
            writer.WriteVarString(alertPayload.Payload);
            writer.WriteVarString(alertPayload.Signature);
        }

        public static byte[] EncodeAlertPayload(AlertPayload alertPayload)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodeAlertPayload(writer, alertPayload);
                return stream.ToArray();
            }
        }

        public static AddressPayload DecodeAddressPayload(BinaryReader reader)
        {
            return new AddressPayload
            (
                NetworkAddresses: reader.ReadList(() => DecodeNetworkAddressWithTime(reader))
            );
        }

        public static AddressPayload DecodeAddressPayload(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            using (var reader = new BinaryReader(stream))
            {
                return DecodeAddressPayload(reader);
            }
        }

        public static void EncodeAddressPayload(BinaryWriter writer, AddressPayload addressPayload)
        {
            writer.WriteList(addressPayload.NetworkAddresses, networkAddress => EncodeNetworkAddressWithTime(writer, networkAddress));
        }

        public static byte[] EncodeAddressPayload(AddressPayload addressPayload)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodeAddressPayload(writer, addressPayload);
                return stream.ToArray();
            }
        }

        public static GetBlocksPayload DecodeGetBlocksPayload(BinaryReader reader)
        {
            return new GetBlocksPayload
            (
                Version: reader.ReadUInt32(),
                BlockLocatorHashes: reader.ReadList(() => reader.ReadUInt256()),
                HashStop: reader.ReadUInt256()
            );
        }

        public static GetBlocksPayload DecodeGetBlocksPayload(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            using (var reader = new BinaryReader(stream))
            {
                return DecodeGetBlocksPayload(reader);
            }
        }

        public static void EncodeGetBlocksPayload(BinaryWriter writer, GetBlocksPayload getBlocksPayload)
        {
            writer.WriteUInt32(getBlocksPayload.Version);
            writer.WriteList(getBlocksPayload.BlockLocatorHashes, locatorHash => writer.WriteUInt256(locatorHash));
            writer.WriteUInt256(getBlocksPayload.HashStop);
        }

        public static byte[] EncodeGetBlocksPayload(GetBlocksPayload getBlocksPayload)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodeGetBlocksPayload(writer, getBlocksPayload);
                return stream.ToArray();
            }
        }

        public static InventoryPayload DecodeInventoryPayload(BinaryReader reader)
        {
            return new InventoryPayload
            (
                InventoryVectors: reader.ReadList(() => DecodeInventoryVector(reader))
            );
        }

        public static InventoryPayload DecodeInventoryPayload(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            using (var reader = new BinaryReader(stream))
            {
                return DecodeInventoryPayload(reader);
            }
        }

        public static void EncodeInventoryPayload(BinaryWriter writer, InventoryPayload invPayload)
        {
            writer.WriteList(invPayload.InventoryVectors, invVector => EncodeInventoryVector(writer, invVector));
        }

        public static byte[] EncodeInventoryPayload(InventoryPayload invPayload)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodeInventoryPayload(writer, invPayload);
                return stream.ToArray();
            }
        }

        public static InventoryVector DecodeInventoryVector(BinaryReader reader)
        {
            return new InventoryVector
            (
                Type: reader.ReadUInt32(),
                Hash: reader.ReadUInt256()
            );
        }

        public static InventoryVector DecodeInventoryVector(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            using (var reader = new BinaryReader(stream))
            {
                return DecodeInventoryVector(reader);
            }
        }

        public static void EncodeInventoryVector(BinaryWriter writer, InventoryVector invVector)
        {
            writer.WriteUInt32(invVector.Type);
            writer.WriteUInt256(invVector.Hash);
        }

        public static byte[] EncodeInventoryVector(InventoryVector invVector)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodeInventoryVector(writer, invVector);
                return stream.ToArray();
            }
        }

        public static Message DecodeMessage(BinaryReader reader)
        {
            var magic = reader.ReadUInt32();
            var command = reader.ReadFixedString(12);
            var payloadSize = reader.ReadUInt32();
            var payloadChecksum = reader.ReadUInt32();
            var payload = reader.ReadExactly(payloadSize.ToIntChecked()).ToImmutableArray();

            return new Message
            (
                Magic: magic,
                Command: command,
                PayloadSize: payloadSize,
                PayloadChecksum: payloadChecksum,
                Payload: payload
            );
        }

        public static Message DecodeMessage(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            using (var reader = new BinaryReader(stream))
            {
                return DecodeMessage(reader);
            }
        }

        public static void EncodeMessage(BinaryWriter writer, Message message)
        {
            writer.WriteUInt32(message.Magic);
            writer.WriteFixedString(12, message.Command);
            writer.WriteUInt32(message.PayloadSize);
            writer.WriteUInt32(message.PayloadChecksum);
            writer.WriteBytes(message.PayloadSize.ToIntChecked(), message.Payload.ToArray());
        }

        public static byte[] EncodeMessage(Message message)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodeMessage(writer, message);
                return stream.ToArray();
            }
        }

        public static NetworkAddress DecodeNetworkAddress(BinaryReader reader)
        {
            return new NetworkAddress
            (
                Services: reader.ReadUInt64(),
                IPv6Address: reader.ReadExactly(16).ToImmutableArray(),
                Port: reader.ReadUInt16BE()
            );
        }

        public static NetworkAddress DecodeNetworkAddress(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            using (var reader = new BinaryReader(stream))
            {
                return DecodeNetworkAddress(reader);
            }
        }

        public static void EncodeNetworkAddress(BinaryWriter writer, NetworkAddress networkAddress)
        {
            writer.WriteUInt64(networkAddress.Services);
            writer.WriteBytes(16, networkAddress.IPv6Address.ToArray());
            writer.WriteUInt16BE(networkAddress.Port);
        }

        public static byte[] EncodeNetworkAddress(NetworkAddress networkAddress)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodeNetworkAddress(writer, networkAddress);
                return stream.ToArray();
            }
        }

        public static NetworkAddressWithTime DecodeNetworkAddressWithTime(BinaryReader reader)
        {
            return new NetworkAddressWithTime
            (
                Time: DateTimeOffset.FromUnixTimeSeconds(reader.ReadUInt32()),
                NetworkAddress: DecodeNetworkAddress(reader)
            );
        }

        public static NetworkAddressWithTime DecodeNetworkAddressWithTime(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            using (var reader = new BinaryReader(stream))
            {
                return DecodeNetworkAddressWithTime(reader);
            }
        }

        public static void EncodeNetworkAddressWithTime(BinaryWriter writer, NetworkAddressWithTime networkAddressWithTime)
        {
            writer.WriteUInt32((uint)networkAddressWithTime.Time.ToUnixTimeSeconds()); //TODO is time LE or BE on network messages?
            EncodeNetworkAddress(writer, networkAddressWithTime.NetworkAddress);
        }

        public static byte[] EncodeNetworkAddressWithTime(NetworkAddressWithTime networkAddressWithTime)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodeNetworkAddressWithTime(writer, networkAddressWithTime);
                return stream.ToArray();
            }
        }

        public static VersionPayload DecodeVersionPayload(BinaryReader reader, int payloadLength)
        {
            var position = reader.BaseStream.Position;

            var versionPayload = new VersionPayload
            (
                ProtocolVersion: reader.ReadUInt32(),
                ServicesBitfield: reader.ReadUInt64(),
                Time: DateTimeOffset.FromUnixTimeSeconds((long)reader.ReadUInt64()),
                RemoteAddress: DecodeNetworkAddress(reader),
                LocalAddress: DecodeNetworkAddress(reader),
                Nonce: reader.ReadUInt64(),
                UserAgent: reader.ReadVarString(),
                StartBlockHeight: reader.ReadUInt32(),
                Relay: false
            );

            var readCount = reader.BaseStream.Position - position;
            if (versionPayload.ProtocolVersion >= VersionPayload.RELAY_VERSION && payloadLength - readCount == 1)
                versionPayload = versionPayload.With(Relay: reader.ReadBool());

            return versionPayload;
        }

        public static VersionPayload DecodeVersionPayload(byte[] bytes, int payloadLength)
        {
            using (var stream = new MemoryStream(bytes))
            using (var reader = new BinaryReader(stream))
            {
                return DecodeVersionPayload(reader, payloadLength);
            }
        }

        public static void EncodeVersionPayload(BinaryWriter writer, VersionPayload versionPayload, bool withRelay)
        {
            writer.WriteUInt32(versionPayload.ProtocolVersion);
            writer.WriteUInt64(versionPayload.ServicesBitfield);
            writer.WriteUInt64((ulong)versionPayload.Time.ToUnixTimeSeconds());
            EncodeNetworkAddress(writer, versionPayload.RemoteAddress);
            EncodeNetworkAddress(writer, versionPayload.LocalAddress);
            writer.WriteUInt64(versionPayload.Nonce);
            writer.WriteVarString(versionPayload.UserAgent);
            writer.WriteUInt32(versionPayload.StartBlockHeight);

            if (withRelay)
                writer.WriteBool(versionPayload.Relay);
        }

        public static byte[] EncodeVersionPayload(VersionPayload versionPayload, bool withRelay)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodeVersionPayload(writer, versionPayload, withRelay);
                return stream.ToArray();
            }
        }

        public static NetworkAddressKey DecodeNetworkAddressKey(BinaryReader reader)
        {
            return new NetworkAddressKey
            (
                IPv6Address: reader.ReadVarBytes().ToImmutableArray(),
                Port: reader.ReadUInt16()
            );
        }

        public static NetworkAddressKey DecodeNetworkAddressKey(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            using (var reader = new BinaryReader(stream))
            {
                return DecodeNetworkAddressKey(reader);
            }
        }

        public static void EncodeNetworkAddressKey(BinaryWriter writer, NetworkAddressKey networkAddressKey)
        {
            writer.WriteVarBytes(networkAddressKey.IPv6Address.ToArray());
            writer.Write(networkAddressKey.Port);
        }

        public static byte[] EncodeNetworkAddressKey(NetworkAddressKey networkAddressKey)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                EncodeNetworkAddressKey(writer, networkAddressKey);
                return stream.ToArray();
            }
        }
    }
}
