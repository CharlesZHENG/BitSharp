using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.ExtensionMethods;
using BitSharp.Node.Domain;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Text;

namespace BitSharp.Node
{
    public static class NodeEncoder
    {
        public static AlertPayload DecodeAlertPayload(Stream stream)
        {
            using (var reader = new BinaryReader(stream, Encoding.ASCII, leaveOpen: true))
            {
                return new AlertPayload
                (
                    Payload: reader.ReadVarString(),
                    Signature: reader.ReadVarString()
                );
            }
        }

        public static AlertPayload DecodeAlertPayload(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            {
                return DecodeAlertPayload(stream);
            }
        }

        public static void EncodeAlertPayload(Stream stream, AlertPayload alertPayload)
        {
            using (var writer = new BinaryWriter(stream, Encoding.ASCII, leaveOpen: true))
            {
                writer.WriteVarString(alertPayload.Payload);
                writer.WriteVarString(alertPayload.Signature);
            }
        }

        public static byte[] EncodeAlertPayload(AlertPayload alertPayload)
        {
            using (var stream = new MemoryStream())
            {
                EncodeAlertPayload(stream, alertPayload);
                return stream.ToArray();
            }
        }

        public static AddressPayload DecodeAddressPayload(Stream stream)
        {
            using (var reader = new BinaryReader(stream, Encoding.ASCII, leaveOpen: true))
            {
                return new AddressPayload
                (
                    NetworkAddresses: reader.ReadList(() => DecodeNetworkAddressWithTime(stream))
                );
            }
        }

        public static AddressPayload DecodeAddressPayload(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            {
                return DecodeAddressPayload(stream);
            }
        }

        public static void EncodeAddressPayload(Stream stream, AddressPayload addressPayload)
        {
            using (var writer = new BinaryWriter(stream, Encoding.ASCII, leaveOpen: true))
            {
                writer.WriteList(addressPayload.NetworkAddresses, networkAddress => EncodeNetworkAddressWithTime(stream, networkAddress));
            }
        }

        public static byte[] EncodeAddressPayload(AddressPayload addressPayload)
        {
            using (var stream = new MemoryStream())
            {
                EncodeAddressPayload(stream, addressPayload);
                return stream.ToArray();
            }
        }

        public static GetBlocksPayload DecodeGetBlocksPayload(Stream stream)
        {
            using (var reader = new BinaryReader(stream, Encoding.ASCII, leaveOpen: true))
            {
                return new GetBlocksPayload
                (
                    Version: reader.ReadUInt32(),
                    BlockLocatorHashes: reader.ReadList(() => reader.ReadUInt256()),
                    HashStop: reader.ReadUInt256()
                );
            }
        }

        public static GetBlocksPayload DecodeGetBlocksPayload(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            {
                return DecodeGetBlocksPayload(stream);
            }
        }

        public static void EncodeGetBlocksPayload(Stream stream, GetBlocksPayload getBlocksPayload)
        {
            using (var writer = new BinaryWriter(stream, Encoding.ASCII, leaveOpen: true))
            {
                writer.WriteUInt32(getBlocksPayload.Version);
                writer.WriteList(getBlocksPayload.BlockLocatorHashes, locatorHash => writer.WriteUInt256(locatorHash));
                writer.WriteUInt256(getBlocksPayload.HashStop);
            }
        }

        public static byte[] EncodeGetBlocksPayload(GetBlocksPayload getBlocksPayload)
        {
            using (var stream = new MemoryStream())
            {
                EncodeGetBlocksPayload(stream, getBlocksPayload);
                return stream.ToArray();
            }
        }

        public static InventoryPayload DecodeInventoryPayload(Stream stream)
        {
            using (var reader = new BinaryReader(stream, Encoding.ASCII, leaveOpen: true))
            {
                return new InventoryPayload
                (
                    InventoryVectors: reader.ReadList(() => DecodeInventoryVector(stream))
                );
            }
        }

        public static InventoryPayload DecodeInventoryPayload(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            {
                return DecodeInventoryPayload(stream);
            }
        }

        public static void EncodeInventoryPayload(Stream stream, InventoryPayload invPayload)
        {
            using (var writer = new BinaryWriter(stream, Encoding.ASCII, leaveOpen: true))
            {
                writer.WriteList(invPayload.InventoryVectors, invVector => EncodeInventoryVector(stream, invVector));
            }
        }

        public static byte[] EncodeInventoryPayload(InventoryPayload invPayload)
        {
            using (var stream = new MemoryStream())
            {
                EncodeInventoryPayload(stream, invPayload);
                return stream.ToArray();
            }
        }

        public static InventoryVector DecodeInventoryVector(Stream stream)
        {
            using (var reader = new BinaryReader(stream, Encoding.ASCII, leaveOpen: true))
            {
                return new InventoryVector
                (
                    Type: reader.ReadUInt32(),
                    Hash: reader.ReadUInt256()
                );
            }
        }

        public static InventoryVector DecodeInventoryVector(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            {
                return DecodeInventoryVector(stream);
            }
        }

        public static void EncodeInventoryVector(Stream stream, InventoryVector invVector)
        {
            using (var writer = new BinaryWriter(stream, Encoding.ASCII, leaveOpen: true))
            {
                writer.WriteUInt32(invVector.Type);
                writer.WriteUInt256(invVector.Hash);
            }
        }

        public static byte[] EncodeInventoryVector(InventoryVector invVector)
        {
            using (var stream = new MemoryStream())
            {
                EncodeInventoryVector(stream, invVector);
                return stream.ToArray();
            }
        }

        public static Message DecodeMessage(Stream stream)
        {
            using (var reader = new BinaryReader(stream, Encoding.ASCII, leaveOpen: true))
            {
                var magic = reader.ReadUInt32();
                var command = reader.ReadFixedString(12);
                var payloadSize = reader.ReadUInt32();
                var payloadChecksum = reader.ReadUInt32();
                var payload = reader.ReadBytes(payloadSize.ToIntChecked()).ToImmutableArray();

                return new Message
                (
                    Magic: magic,
                    Command: command,
                    PayloadSize: payloadSize,
                    PayloadChecksum: payloadChecksum,
                    Payload: payload
                );
            }
        }

        public static Message DecodeMessage(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            {
                return DecodeMessage(stream);
            }
        }

        public static void EncodeMessage(Stream stream, Message message)
        {
            using (var writer = new BinaryWriter(stream, Encoding.ASCII, leaveOpen: true))
            {
                writer.WriteUInt32(message.Magic);
                writer.WriteFixedString(12, message.Command);
                writer.WriteUInt32(message.PayloadSize);
                writer.WriteUInt32(message.PayloadChecksum);
                writer.WriteBytes(message.PayloadSize.ToIntChecked(), message.Payload.ToArray());
            }
        }

        public static byte[] EncodeMessage(Message message)
        {
            using (var stream = new MemoryStream())
            {
                EncodeMessage(stream, message);
                return stream.ToArray();
            }
        }

        public static NetworkAddress DecodeNetworkAddress(Stream stream)
        {
            using (var reader = new BinaryReader(stream, Encoding.ASCII, leaveOpen: true))
            {
                return new NetworkAddress
                (
                    Services: reader.ReadUInt64(),
                    IPv6Address: reader.ReadBytes(16).ToImmutableArray(),
                    Port: reader.ReadUInt16BE()
                );
            }
        }

        public static NetworkAddress DecodeNetworkAddress(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            {
                return DecodeNetworkAddress(stream);
            }
        }

        public static void EncodeNetworkAddress(Stream stream, NetworkAddress networkAddress)
        {
            using (var writer = new BinaryWriter(stream, Encoding.ASCII, leaveOpen: true))
            {
                writer.WriteUInt64(networkAddress.Services);
                writer.WriteBytes(16, networkAddress.IPv6Address.ToArray());
                writer.WriteUInt16BE(networkAddress.Port);
            }
        }

        public static byte[] EncodeNetworkAddress(NetworkAddress networkAddress)
        {
            using (var stream = new MemoryStream())
            {
                EncodeNetworkAddress(stream, networkAddress);
                return stream.ToArray();
            }
        }

        public static NetworkAddressWithTime DecodeNetworkAddressWithTime(Stream stream)
        {
            using (var reader = new BinaryReader(stream, Encoding.ASCII, leaveOpen: true))
            {
                return new NetworkAddressWithTime
                (
                    Time: reader.ReadUInt32(),
                    NetworkAddress: DecodeNetworkAddress(stream)
                );
            }
        }

        public static NetworkAddressWithTime DecodeNetworkAddressWithTime(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            {
                return DecodeNetworkAddressWithTime(stream);
            }
        }

        public static void EncodeNetworkAddressWithTime(Stream stream, NetworkAddressWithTime networkAddressWithTime)
        {
            using (var writer = new BinaryWriter(stream, Encoding.ASCII, leaveOpen: true))
            {
                writer.WriteUInt32(networkAddressWithTime.Time);
                EncodeNetworkAddress(stream, networkAddressWithTime.NetworkAddress);
            }
        }

        public static byte[] EncodeNetworkAddressWithTime(NetworkAddressWithTime networkAddressWithTime)
        {
            using (var stream = new MemoryStream())
            {
                EncodeNetworkAddressWithTime(stream, networkAddressWithTime);
                return stream.ToArray();
            }
        }

        public static VersionPayload DecodeVersionPayload(Stream stream, int payloadLength)
        {
            using (var reader = new BinaryReader(stream, Encoding.ASCII, leaveOpen: true))
            {
                var position = stream.Position;

                var versionPayload = new VersionPayload
                (
                    ProtocolVersion: reader.ReadUInt32(),
                    ServicesBitfield: reader.ReadUInt64(),
                    UnixTime: reader.ReadUInt64(),
                    RemoteAddress: DecodeNetworkAddress(stream),
                    LocalAddress: DecodeNetworkAddress(stream),
                    Nonce: reader.ReadUInt64(),
                    UserAgent: reader.ReadVarString(),
                    StartBlockHeight: reader.ReadUInt32(),
                    Relay: false
                );

                var readCount = stream.Position - position;
                if (versionPayload.ProtocolVersion >= VersionPayload.RELAY_VERSION && payloadLength - readCount == 1)
                    versionPayload = versionPayload.With(Relay: reader.ReadBool());

                return versionPayload;
            }
        }

        public static VersionPayload DecodeVersionPayload(byte[] bytes, int payloadLength)
        {
            using (var stream = new MemoryStream(bytes))
            {
                return DecodeVersionPayload(stream, payloadLength);
            }
        }

        public static void EncodeVersionPayload(Stream stream, VersionPayload versionPayload, bool withRelay)
        {
            using (var writer = new BinaryWriter(stream, Encoding.ASCII, leaveOpen: true))
            {
                writer.WriteUInt32(versionPayload.ProtocolVersion);
                writer.WriteUInt64(versionPayload.ServicesBitfield);
                writer.WriteUInt64(versionPayload.UnixTime);
                EncodeNetworkAddress(stream, versionPayload.RemoteAddress);
                EncodeNetworkAddress(stream, versionPayload.LocalAddress);
                writer.WriteUInt64(versionPayload.Nonce);
                writer.WriteVarString(versionPayload.UserAgent);
                writer.WriteUInt32(versionPayload.StartBlockHeight);

                if (withRelay)
                    writer.WriteBool(versionPayload.Relay);
            }
        }

        public static byte[] EncodeVersionPayload(VersionPayload versionPayload, bool withRelay)
        {
            using (var stream = new MemoryStream())
            {
                EncodeVersionPayload(stream, versionPayload, withRelay);
                return stream.ToArray();
            }
        }

        public static NetworkAddressKey DecodeNetworkAddressKey(Stream stream)
        {
            using (var reader = new BinaryReader(stream, Encoding.ASCII, leaveOpen: true))
            {
                return new NetworkAddressKey
                (
                    IPv6Address: reader.ReadVarBytes().ToImmutableArray(),
                    Port: reader.ReadUInt16()
                );
            }
        }

        public static NetworkAddressKey DecodeNetworkAddressKey(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes))
            {
                return DecodeNetworkAddressKey(stream);
            }
        }

        public static void EncodeNetworkAddressKey(Stream stream, NetworkAddressKey networkAddressKey)
        {
            using (var writer = new BinaryWriter(stream, Encoding.ASCII, leaveOpen: true))
            {
                writer.WriteVarBytes(networkAddressKey.IPv6Address.ToArray());
                writer.Write(networkAddressKey.Port);
            }
        }

        public static byte[] EncodeNetworkAddressKey(NetworkAddressKey networkAddressKey)
        {
            using (var stream = new MemoryStream())
            {
                EncodeNetworkAddressKey(stream, networkAddressKey);
                return stream.ToArray();
            }
        }
    }
}
