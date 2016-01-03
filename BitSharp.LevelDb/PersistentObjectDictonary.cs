using LevelDB;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace BitSharp.LevelDb
{
    public class PersistentObjectDictonary<TKey, TValue> : IDictionary<TKey, TValue>, ICollection<KeyValuePair<TKey, TValue>>, IEnumerable<KeyValuePair<TKey, TValue>>, IEnumerable, IDisposable
    {
        private readonly string directory;
        private readonly string dbFile;
        private readonly Func<TKey, byte[]> keyEncoder;
        private readonly Func<byte[], TKey> keyDecoder;
        private readonly Func<TValue, byte[]> valueEncoder;
        private readonly Func<byte[], TValue> valueDecoder;
        private readonly DB db;

        private bool isDisposed;

        public PersistentObjectDictonary(string directory, Func<TKey, byte[]> keyEncoder, Func<byte[], TKey> keyDecoder, Func<TValue, byte[]> valueEncoder, Func<byte[], TValue> valueDecoder)
        {
            this.directory = directory;
            this.dbFile = Path.Combine(directory, "DB");
            this.keyEncoder = keyEncoder;
            this.keyDecoder = keyDecoder;
            this.valueEncoder = valueEncoder;
            this.valueDecoder = valueDecoder;
            db = DB.Open(dbFile);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!isDisposed && disposing)
            {
                db.Dispose();

                isDisposed = true;
            }
        }

        public void Add(TKey key, TValue value)
        {
            db.Put(new WriteOptions(), EncodeKey(key), EncodeValue(value));
        }

        public bool ContainsKey(TKey key)
        {
            Slice value;
            return db.TryGet(new ReadOptions(), EncodeKey(key), out value);
        }

        public ICollection<TKey> Keys
        {
            get { Debugger.Break(); throw new NotImplementedException(); }
        }

        public bool Remove(TKey key)
        {
            if (!ContainsKey(key))
                return false;

            db.Delete(new WriteOptions(), EncodeKey(key));
            return true;
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            Slice rawValue;
            if (db.TryGet(new ReadOptions(), EncodeKey(key), out rawValue))
            {
                value = DecodeValue(rawValue.ToArray());
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
            get { Debugger.Break(); throw new NotImplementedException(); }
        }

        public TValue this[TKey key]
        {
            get
            {
                Slice value;
                if (db.TryGet(new ReadOptions(), EncodeKey(key), out value))
                    return DecodeValue(value.ToArray());
                else
                    return default(TValue);
            }
            set
            {
                db.Put(new WriteOptions(), EncodeKey(key), EncodeValue(value));
            }
        }

        public void Add(KeyValuePair<TKey, TValue> item)
        {
            db.Put(new WriteOptions(), EncodeKey(item.Key), EncodeValue(item.Value));
        }

        public void Clear()
        {
            Debugger.Break();
            throw new NotImplementedException();
        }

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            Debugger.Break();
            throw new NotImplementedException();
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
            get { Debugger.Break(); throw new NotImplementedException(); }
        }

        public bool IsReadOnly { get; } = false;

        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            Debugger.Break();
            throw new NotImplementedException();
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            using (var snapshot = db.GetSnapshot())
            {
                var readOptions = new ReadOptions { Snapshot = snapshot };

                using (var iterator = db.NewIterator(readOptions))
                {
                    iterator.SeekToFirst();
                    while (iterator.Valid())
                    {
                        var key = iterator.Key().ToArray();
                        var value = iterator.Value().ToArray();
                        yield return new KeyValuePair<TKey, TValue>(DecodeKey(key), DecodeValue(value));

                        iterator.Next();
                    }
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        private byte[] EncodeKey(TKey key)
        {
            return keyEncoder(key);
        }

        private TKey DecodeKey(byte[] key)
        {
            return keyDecoder(key);
        }

        private byte[] EncodeValue(TValue value)
        {
            return valueEncoder(value);
        }

        private TValue DecodeValue(byte[] value)
        {
            return valueDecoder(value);
        }

        //public class PersistentObjectDictionaryKeyCollection : ICollection<TKey>
        //{
        //    private readonly PersistentDictionaryKeyCollection<string, PersistentBlob> keyCollection;
        //    private readonly Func<TKey, byte[]> keyEncoder;
        //    private readonly Func<byte[], TKey> keyDecoder;

        //    public PersistentObjectDictionaryKeyCollection(PersistentDictionaryKeyCollection<string, PersistentBlob> keyCollection, Func<TKey, byte[]> keyEncoder, Func<byte[], TKey> keyDecoder)
        //    {
        //        this.keyCollection = keyCollection;
        //        this.keyEncoder = keyEncoder;
        //        this.keyDecoder = keyDecoder;
        //    }

        //    public void Add(TKey item)
        //    {
        //        keyCollection.Add(EncodeKey(item));
        //    }

        //    public void Clear()
        //    {
        //        keyCollection.Clear();
        //    }

        //    public bool Contains(TKey item)
        //    {
        //        return keyCollection.Contains(EncodeKey(item));
        //    }

        //    public void CopyTo(TKey[] array, int arrayIndex)
        //    {
        //        foreach (var key in this)
        //        {
        //            array[arrayIndex] = key;
        //            arrayIndex++;
        //        }
        //    }

        //    public int Count => keyCollection.Count;

        //    public bool IsReadOnly => keyCollection.IsReadOnly;

        //    public bool Remove(TKey item)
        //    {
        //        return keyCollection.Remove(EncodeKey(item));
        //    }

        //    public IEnumerator<TKey> GetEnumerator()
        //    {
        //        foreach (var key in keyCollection)
        //            yield return DecodeKey(key);

        //    }

        //    System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        //    {
        //        return GetEnumerator();
        //    }

        //    private string EncodeKey(TKey key)
        //    {
        //        return Base32Encoder.Encode(keyEncoder(key));
        //    }

        //    private TKey DecodeKey(string key)
        //    {
        //        return keyDecoder(Base32Encoder.Decode(key));
        //    }
        //}

        //public class PersistentObjectDictionaryValueCollection : ICollection<TValue>
        //{
        //    private readonly PersistentDictionaryValueCollection<string, PersistentBlob> valueCollection;
        //    private readonly Func<TValue, byte[]> valueEncoder;
        //    private readonly Func<byte[], TValue> valueDecoder;

        //    public PersistentObjectDictionaryValueCollection(PersistentDictionaryValueCollection<string, PersistentBlob> valueCollection, Func<TValue, byte[]> valueEncoder, Func<byte[], TValue> valueDecoder)
        //    {
        //        this.valueCollection = valueCollection;
        //        this.valueEncoder = valueEncoder;
        //        this.valueDecoder = valueDecoder;
        //    }

        //    public void Add(TValue item)
        //    {
        //        valueCollection.Add(EncodeValue(item));
        //    }

        //    public void Clear()
        //    {
        //        valueCollection.Clear();
        //    }

        //    public bool Contains(TValue item)
        //    {
        //        return valueCollection.Contains(EncodeValue(item));
        //    }

        //    public void CopyTo(TValue[] array, int arrayIndex)
        //    {
        //        foreach (var value in this)
        //        {
        //            array[arrayIndex] = value;
        //            arrayIndex++;
        //        }
        //    }

        //    public int Count => valueCollection.Count;

        //    public bool IsReadOnly => valueCollection.IsReadOnly;

        //    public bool Remove(TValue item)
        //    {
        //        return valueCollection.Remove(EncodeValue(item));
        //    }

        //    public IEnumerator<TValue> GetEnumerator()
        //    {
        //        foreach (var value in valueCollection)
        //            yield return DecodeValue(value);
        //    }

        //    System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        //    {
        //        return GetEnumerator();
        //    }

        //    private PersistentBlob EncodeValue(TValue value)
        //    {
        //        return new PersistentBlob(valueEncoder(value));
        //    }

        //    private TValue DecodeValue(PersistentBlob value)
        //    {
        //        return valueDecoder(value.GetBytes());
        //    }
        //}
    }
}
