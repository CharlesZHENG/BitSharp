using BitSharp.Common;
using System.Collections;
using System.Collections.Generic;
using System.Data.Linq;
using System.Linq;

namespace BitSharp.Core.Domain
{
    public enum OutputState
    {
        Unspent,
        Spent
    }

    public class OutputStates : IEnumerable<OutputState>
    {
        private readonly ImmutableBitArray bitArray;

        public static OutputStates Empty { get; } = new OutputStates(0, OutputState.Spent);

        public OutputStates(int length, OutputState state)
        {
            this.bitArray = new ImmutableBitArray(length, Encode(state));
        }

        public OutputStates(byte[] bytes, int length)
        {
            this.bitArray = new ImmutableBitArray(bytes, length);
        }

        public OutputStates(ImmutableBitArray bitArray)
        {
            this.bitArray = bitArray;
        }

        public OutputState this[int index] => Decode(this.bitArray[index]);

        public int Length => this.bitArray.Length;

        public OutputStates Set(int index, OutputState value)
        {
            return new OutputStates(this.bitArray.Set(index, Encode(value)));
        }

        public IEnumerator<OutputState> GetEnumerator()
        {
            for (var i = 0; i < this.bitArray.Length; i++)
                yield return Decode(this.bitArray[i]);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.bitArray.GetEnumerator();
        }

        public byte[] ToByteArray()
        {
            return this.bitArray.ToByteArray();
        }

        private bool Encode(OutputState state)
        {
            return state == OutputState.Unspent;
        }

        private OutputState Decode(bool value)
        {
            return value ? OutputState.Unspent : OutputState.Spent;
        }

        //TODO only exists for tests
        public override bool Equals(object obj)
        {
            if (!(obj is OutputStates))
                return false;

            var other = (OutputStates)obj;
            return other.bitArray.SequenceEqual(this.bitArray);
        }
    }
}
