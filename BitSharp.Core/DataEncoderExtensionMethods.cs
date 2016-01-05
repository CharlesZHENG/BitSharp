using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Text;

namespace BitSharp.Core.ExtensionMethods
{
    public static class DataEncoderExtensionMethods
    {
        #region Reader Methods
        public static void ReadExactly(this BinaryReader reader, byte[] buffer, int index, int count)
        {
            if (count == 0)
                return;

            if (count != reader.Read(buffer, index, count))
                throw new InvalidOperationException();
        }

        public static byte[] ReadExactly(this BinaryReader reader, int count)
        {
            var buffer = new byte[count];
            if (count == 0)
                return buffer;

            if (count != reader.Read(buffer, 0, count))
                throw new InvalidOperationException();

            return buffer;
        }

        public static bool ReadBool(this BinaryReader reader)
        {
            return reader.ReadByte() != 0;
        }

        public static UInt16 ReadUInt16BE(this BinaryReader reader)
        {
            using (var reverse = reader.ReverseRead(2))
                return reverse.ReadUInt16();
        }

        public static UInt256 ReadUInt256(this BinaryReader reader)
        {
            return new UInt256(reader.ReadExactly(32));
        }

        public static UInt64 ReadVarInt(this BinaryReader reader)
        {
            var value = reader.ReadByte();
            if (value < 0xFD)
                return value;
            else if (value == 0xFD)
                return reader.ReadUInt16();
            else if (value == 0xFE)
                return reader.ReadUInt32();
            else if (value == 0xFF)
                return reader.ReadUInt64();
            else
                throw new Exception();
        }

        public static UInt64 ReadVarInt(this BinaryReader reader, ref byte[] bytes, ref int offset)
        {
            DataDecoder.SizeAtLeast(ref bytes, offset + 1);
            reader.ReadExactly(bytes, offset, 1);
            UInt64 value = bytes[offset];
            offset += 1;

            if (value < 0xFD)
            {
                return value;
            }
            else if (value == 0xFD)
            {
                DataDecoder.SizeAtLeast(ref bytes, offset + 2);
                reader.ReadExactly(bytes, offset, 2);
                value = Bits.ToUInt16(bytes, offset);
                offset += 2;
                return value;
            }
            else if (value == 0xFE)
            {
                DataDecoder.SizeAtLeast(ref bytes, offset + 4);
                reader.ReadExactly(bytes, offset, 4);
                value = Bits.ToUInt32(bytes, offset);
                offset += 4;
                return value;
            }
            else if (value == 0xFF)
            {
                DataDecoder.SizeAtLeast(ref bytes, offset + 8);
                reader.ReadExactly(bytes, offset, 8);
                value = Bits.ToUInt64(bytes, offset);
                offset += 8;
                return value;
            }
            else
                throw new Exception();
        }

        public static UInt64 ReadVarInt(this byte[] buffer, ref int offset)
        {
            UInt64 value = buffer[offset];
            offset += 1;

            if (value < 0xFD)
            {
                return value;
            }
            else if (value == 0xFD)
            {
                value = Bits.ToUInt16(buffer, offset);
                offset += 2;
                return value;
            }
            else if (value == 0xFE)
            {
                value = Bits.ToUInt32(buffer, offset);
                offset += 4;
                return value;
            }
            else if (value == 0xFF)
            {
                value = Bits.ToUInt64(buffer, offset);
                offset += 8;
                return value;
            }
            else
                throw new Exception();
        }

        public static byte[] ReadVarBytes(this BinaryReader reader)
        {
            var length = reader.ReadVarInt().ToIntChecked();
            return reader.ReadExactly(length);
        }

        public static byte[] ReadVarBytes(this byte[] buffer, ref int offset)
        {
            var length = buffer.ReadVarInt(ref offset).ToIntChecked();

            var value = new byte[length];
            Buffer.BlockCopy(buffer, offset, value, 0, length);
            offset += length;

            return value;
        }

        public static ImmutableArray<byte> ReadVarBytesImmutable(this byte[] buffer, ref int offset)
        {
            var length = buffer.ReadVarInt(ref offset).ToIntChecked();

            var value = ImmutableArray.Create(buffer, offset, length);
            offset += length;

            return value;
        }

        public static string ReadVarString(this BinaryReader reader)
        {
            var rawBytes = reader.ReadVarBytes();
            return Encoding.ASCII.GetString(rawBytes);
        }

        public static string ReadFixedString(this BinaryReader reader, int length)
        {
            var encoded = reader.ReadExactly(length);
            // ignore trailing null bytes in a fixed length string
            var encodedTrimmed = encoded.TakeWhile(x => x != 0).ToArray();
            var decoded = Encoding.ASCII.GetString(encodedTrimmed);

            return decoded;
        }

        public static ImmutableArray<T> ReadList<T>(this BinaryReader reader, Func<T> decode)
        {
            var length = reader.ReadVarInt().ToIntChecked();

            var list = ImmutableArray.CreateBuilder<T>(length);
            for (var i = 0; i < length; i++)
            {
                list.Add(decode());
            }

            return list.ToImmutable();
        }

        public static T[] ReadArray<T>(this BinaryReader reader, Func<T> decode)
        {
            var length = reader.ReadVarInt().ToIntChecked();

            var list = new T[length];
            for (var i = 0; i < length; i++)
            {
                list[i] = decode();
            }

            return list;
        }

        private static BinaryReader ReverseRead(this BinaryReader reader, int length)
        {
            var bytes = reader.ReadExactly(length);
            Array.Reverse(bytes);

            var stream = new MemoryStream(bytes);
            return new BinaryReader(stream, Encoding.ASCII, leaveOpen: false);
        }
        #endregion

        #region Writer Methods
        public static void WriteBool(this BinaryWriter writer, bool value)
        {
            writer.Write((byte)(value ? 1 : 0));
        }

        public static void Write1Byte(this BinaryWriter writer, Byte value)
        {
            writer.Write(value);
        }

        public static void WriteUInt16(this BinaryWriter writer, UInt16 value)
        {
            writer.Write(value);
        }

        public static void WriteUInt16BE(this BinaryWriter writer, UInt16 value)
        {
            writer.ReverseWrite(2, reverseWriter => reverseWriter.WriteUInt16(value));
        }

        public static void WriteUInt32(this BinaryWriter writer, UInt32 value)
        {
            writer.Write(value);
        }

        public static void WriteInt32(this BinaryWriter writer, Int32 value)
        {
            writer.Write(value);
        }

        public static void WriteUInt64(this BinaryWriter writer, UInt64 value)
        {
            writer.Write(value);
        }

        public static void WriteInt64(this BinaryWriter writer, Int64 value)
        {
            writer.Write(value);
        }

        public static void WriteUInt256(this BinaryWriter writer, UInt256 value)
        {
            writer.Write(value.ToByteArray());
        }

        public static void WriteBytes(this BinaryWriter writer, byte[] value)
        {
            writer.Write(value);
        }

        public static void WriteBytes(this BinaryWriter writer, int length, byte[] value)
        {
            if (value.Length != length)
                throw new ArgumentException();

            writer.WriteBytes(value);
        }

        public static void WriteVarInt(this BinaryWriter writer, UInt64 value)
        {
            if (value < 0xFD)
            {
                writer.Write1Byte((Byte)value);
            }
            else if (value <= 0xFFFF)
            {
                writer.Write1Byte(0xFD);
                writer.WriteUInt16((UInt16)value);
            }
            else if (value <= 0xFFFFFFFF)
            {
                writer.Write1Byte(0xFE);
                writer.WriteUInt32((UInt32)value);
            }
            else
            {
                writer.Write1Byte(0xFF);
                writer.WriteUInt64(value);
            }
        }

        public static void WriteVarBytes(this BinaryWriter writer, byte[] value)
        {
            writer.WriteVarInt((UInt64)value.Length);
            writer.WriteBytes(value.Length, value);
        }

        public static void WriteVarString(this BinaryWriter writer, string value)
        {
            var encoded = Encoding.ASCII.GetBytes(value);
            writer.WriteVarBytes(encoded);
        }

        public static void WriteFixedString(this BinaryWriter writer, int length, string value)
        {
            if (value.Length < length)
                value = value.PadRight(length, '\0');
            if (value.Length != length)
                throw new ArgumentException();

            var encoded = Encoding.ASCII.GetBytes(value);
            writer.WriteBytes(encoded.Length, encoded);
        }

        public static void WriteList<T>(this BinaryWriter writer, IReadOnlyList<T> list, Action<T> encode)
        {
            writer.WriteVarInt((UInt64)list.Count);

            for (var i = 0; i < list.Count; i++)
            {
                encode(list[i]);
            }
        }

        public static void WriteArray<T>(this BinaryWriter writer, T[] list, Action<T> encode)
        {
            writer.WriteVarInt((UInt64)list.Length);

            for (var i = 0; i < list.Length; i++)
            {
                encode(list[i]);
            }
        }

        private static void ReverseWrite(this BinaryWriter writer, int length, Action<BinaryWriter> write)
        {
            var bytes = new byte[length];
            using (var stream = new MemoryStream(bytes))
            using (var reverseWriter = new BinaryWriter(stream))
            {
                write(reverseWriter);

                // verify that the correct amount of bytes were writtern
                if (reverseWriter.BaseStream.Position != length)
                    throw new InvalidOperationException();
            }
            Array.Reverse(bytes);

            writer.WriteBytes(bytes);
        }
        #endregion
    }
}
