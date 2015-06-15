using Base32;
using Microsoft.Isam.Esent.Collections.Generic;
using System;
using System.Collections;
using System.Collections.Generic;

namespace BitSharp.Esent
{
    public class PersistentObjectDictonary<TKey, TValue> : IDictionary<TKey, TValue>, ICollection<KeyValuePair<TKey, TValue>>, IEnumerable<KeyValuePair<TKey, TValue>>, IEnumerable, IDisposable
    {
        private readonly string directory;
        private readonly Func<TKey, byte[]> keyEncoder;
        private readonly Func<byte[], TKey> keyDecoder;
        private readonly Func<TValue, byte[]> valueEncoder;
        private readonly Func<byte[], TValue> valueDecoder;
        private readonly PersistentDictionary<string, PersistentBlob> dict;

        public PersistentObjectDictonary(string directory, Func<TKey, byte[]> keyEncoder, Func<byte[], TKey> keyDecoder, Func<TValue, byte[]> valueEncoder, Func<byte[], TValue> valueDecoder)
        {
            this.directory = directory;
            this.keyEncoder = keyEncoder;
            this.keyDecoder = keyDecoder;
            this.valueEncoder = valueEncoder;
            this.valueDecoder = valueDecoder;
            this.dict = new PersistentDictionary<string, PersistentBlob>(directory);
        }

        public void Dispose()
        {
            this.dict.Dispose();
        }

        public void Add(TKey key, TValue value)
        {
            this.dict.Add(EncodeKey(key), EncodeValue(value));
        }

        public bool ContainsKey(TKey key)
        {
            return this.dict.ContainsKey(EncodeKey(key));
        }

        public ICollection<TKey> Keys
        {
            get { return new PersistentObjectDictionaryKeyCollection(this.dict.Keys, keyEncoder, keyDecoder); }
        }

        public bool Remove(TKey key)
        {
            return this.dict.Remove(EncodeKey(key));
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            PersistentBlob blobValue;
            if (this.dict.TryGetValue(EncodeKey(key), out blobValue))
            {
                value = DecodeValue(blobValue);
                return true;
            }
            else
            {
                value = default(TValue);
                return false;
            }
        }

        public ICollection<TValue> Values
        {
            get { return new PersistentObjectDictionaryValueCollection(this.dict.Values, valueEncoder, valueDecoder); }
        }

        public TValue this[TKey key]
        {
            get
            {
                var blobValue = this.dict[EncodeKey(key)];
                if (blobValue != null)
                    return DecodeValue(blobValue);
                else
                    return default(TValue);
            }
            set
            {
                this.dict[EncodeKey(key)] = EncodeValue(value);
            }
        }

        public void Add(KeyValuePair<TKey, TValue> item)
        {
            this.dict.Add(new KeyValuePair<string, PersistentBlob>(EncodeKey(item.Key), EncodeValue(item.Value)));
        }

        public void Clear()
        {
            this.dict.Clear();
        }

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            return this.dict.Contains(new KeyValuePair<string, PersistentBlob>(EncodeKey(item.Key), EncodeValue(item.Value)));
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            foreach (var item in this)
            {
                array[arrayIndex] = item;
                arrayIndex++;
            }
        }

        public int Count
        {
            get { return this.dict.Count; }
        }

        public bool IsReadOnly
        {
            get { return this.dict.IsReadOnly; }
        }

        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            return this.dict.Remove(new KeyValuePair<string, PersistentBlob>(EncodeKey(item.Key), EncodeValue(item.Value)));
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            foreach (var item in this.dict)
                yield return new KeyValuePair<TKey, TValue>(DecodeKey(item.Key), DecodeValue(item.Value));
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        private string EncodeKey(TKey key)
        {
            return Base32Encoder.Encode(keyEncoder(key));
        }

        private TKey DecodeKey(string key)
        {
            return keyDecoder(Base32Encoder.Decode(key));
        }

        private PersistentBlob EncodeValue(TValue value)
        {
            return new PersistentBlob(valueEncoder(value));
        }

        private TValue DecodeValue(PersistentBlob value)
        {
            return valueDecoder(value.GetBytes());
        }

        public class PersistentObjectDictionaryKeyCollection : ICollection<TKey>
        {
            private readonly PersistentDictionaryKeyCollection<string, PersistentBlob> keyCollection;
            private readonly Func<TKey, byte[]> keyEncoder;
            private readonly Func<byte[], TKey> keyDecoder;

            public PersistentObjectDictionaryKeyCollection(PersistentDictionaryKeyCollection<string, PersistentBlob> keyCollection, Func<TKey, byte[]> keyEncoder, Func<byte[], TKey> keyDecoder)
            {
                this.keyCollection = keyCollection;
                this.keyEncoder = keyEncoder;
                this.keyDecoder = keyDecoder;
            }

            public void Add(TKey item)
            {
                this.keyCollection.Add(EncodeKey(item));
            }

            public void Clear()
            {
                this.keyCollection.Clear();
            }

            public bool Contains(TKey item)
            {
                return this.keyCollection.Contains(EncodeKey(item));
            }

            public void CopyTo(TKey[] array, int arrayIndex)
            {
                foreach (var key in this)
                {
                    array[arrayIndex] = key;
                    arrayIndex++;
                }
            }

            public int Count
            {
                get { return this.keyCollection.Count; }
            }

            public bool IsReadOnly
            {
                get { return this.keyCollection.IsReadOnly; }
            }

            public bool Remove(TKey item)
            {
                return this.keyCollection.Remove(EncodeKey(item));
            }

            public IEnumerator<TKey> GetEnumerator()
            {
                foreach (var key in this.keyCollection)
                    yield return DecodeKey(key);

            }

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            {
                return this.GetEnumerator();
            }

            private string EncodeKey(TKey key)
            {
                return Base32Encoder.Encode(keyEncoder(key));
            }

            private TKey DecodeKey(string key)
            {
                return keyDecoder(Base32Encoder.Decode(key));
            }
        }

        public class PersistentObjectDictionaryValueCollection : ICollection<TValue>
        {
            private readonly PersistentDictionaryValueCollection<string, PersistentBlob> valueCollection;
            private readonly Func<TValue, byte[]> valueEncoder;
            private readonly Func<byte[], TValue> valueDecoder;

            public PersistentObjectDictionaryValueCollection(PersistentDictionaryValueCollection<string, PersistentBlob> valueCollection, Func<TValue, byte[]> valueEncoder, Func<byte[], TValue> valueDecoder)
            {
                this.valueCollection = valueCollection;
                this.valueEncoder = valueEncoder;
                this.valueDecoder = valueDecoder;
            }

            public void Add(TValue item)
            {
                this.valueCollection.Add(EncodeValue(item));
            }

            public void Clear()
            {
                this.valueCollection.Clear();
            }

            public bool Contains(TValue item)
            {
                return this.valueCollection.Contains(EncodeValue(item));
            }

            public void CopyTo(TValue[] array, int arrayIndex)
            {
                foreach (var value in this)
                {
                    array[arrayIndex] = value;
                    arrayIndex++;
                }
            }

            public int Count
            {
                get { return this.valueCollection.Count; }
            }

            public bool IsReadOnly
            {
                get { return this.valueCollection.IsReadOnly; }
            }

            public bool Remove(TValue item)
            {
                return this.valueCollection.Remove(EncodeValue(item));
            }

            public IEnumerator<TValue> GetEnumerator()
            {
                foreach (var value in this.valueCollection)
                    yield return DecodeValue(value);
            }

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            {
                return this.GetEnumerator();
            }

            private PersistentBlob EncodeValue(TValue value)
            {
                return new PersistentBlob(valueEncoder(value));
            }

            private TValue DecodeValue(PersistentBlob value)
            {
                return valueDecoder(value.GetBytes());
            }
        }
    }
}
