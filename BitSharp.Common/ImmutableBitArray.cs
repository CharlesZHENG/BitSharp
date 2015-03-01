using System.Collections;
using System.Collections.Generic;

namespace BitSharp.Common
{
    public class ImmutableBitArray : IEnumerable<bool>
    {
        private readonly BitArray bitArray;

        public ImmutableBitArray(int length, bool defaultValue)
        {
            this.bitArray = new BitArray(length, defaultValue);
        }

        public ImmutableBitArray(BitArray bitArray)
        {
            this.bitArray = (BitArray)bitArray.Clone();
        }

        public ImmutableBitArray(bool[] values)
        {
            this.bitArray = new BitArray(values);
        }

        public ImmutableBitArray(byte[] bytes, int length)
        {
            this.bitArray = new BitArray(bytes);
            this.bitArray.Length = length;
        }

        private ImmutableBitArray(BitArray bitArray, bool clone)
        {
            this.bitArray = clone ? (BitArray)bitArray.Clone() : bitArray;
        }

        public bool this[int index]
        {
            get { return this.bitArray[index]; }
        }

        public int Length
        {
            get { return this.bitArray.Length; }
        }

        public ImmutableBitArray Set(int index, bool value)
        {
            var bitArray = (BitArray)this.bitArray.Clone();
            bitArray[index] = value;
            return new ImmutableBitArray(bitArray, clone: false);
        }

        public IEnumerator<bool> GetEnumerator()
        {
            for (var i = 0; i < this.bitArray.Length; i++)
                yield return this.bitArray[i];
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.bitArray.GetEnumerator();
        }

        public byte[] ToByteArray()
        {
            var byteLength = (this.bitArray.Length + 7) / 8;
            var bytes = new byte[byteLength];
            
            this.bitArray.CopyTo(bytes, 0);
            
            return bytes;
        }
    }
}
