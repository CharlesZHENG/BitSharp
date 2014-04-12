﻿using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BitSharp.Common;
using System.Security.Cryptography;

namespace BitSharp.Common.ExtensionMethods
{
    public static class ExtensionMethods
    {
        private static readonly Random random = new Random();

        public static byte[] Concat(this byte[] first, byte[] second)
        {
            var buffer = new byte[first.Length + second.Length];
            Buffer.BlockCopy(first, 0, buffer, 0, first.Length);
            Buffer.BlockCopy(second, 0, buffer, first.Length, second.Length);
            return buffer;
        }

        public static byte[] Concat(this byte[] first, byte second)
        {
            var buffer = new byte[first.Length + 1];
            Buffer.BlockCopy(first, 0, buffer, 0, first.Length);
            buffer[buffer.Length - 1] = second;
            return buffer;
        }

        public static IEnumerable<T> Concat<T>(this IEnumerable<T> first, T second)
        {
            foreach (var item in first)
                yield return item;

            yield return second;
        }

        // ToHexNumberString    prints out hex bytes in reverse order, as they are internally little-endian but big-endian is what people use:
        //                      bytes 0xEE,0xFF would represent what a person would write down as 0xFFEE

        // ToHexNumberData      prints out hex bytes in order

        public static string ToHexNumberString(this byte[] value)
        {
            return string.Format("0x{0}", Bits.ToString(value.Reverse().ToArray()).Replace("-", "").ToLower());
        }

        public static string ToHexNumberString(this IEnumerable<byte> value)
        {
            return ToHexNumberString(value.ToArray());
        }

        public static string ToHexNumberString(this UInt256 value)
        {
            return ToHexNumberString(value.ToByteArray());
        }

        public static string ToHexNumberString(this BigInteger value)
        {
            return ToHexNumberString(value.ToByteArray());
        }

        public static string ToHexDataString(this byte[] value)
        {
            return string.Format("[{0}]", Bits.ToString(value).Replace("-", ",").ToLower());
        }

        public static string ToHexDataString(this IEnumerable<byte> value)
        {
            return ToHexDataString(value.ToArray());
        }

        public static string ToHexDataString(this UInt256 value)
        {
            return ToHexDataString(value.ToByteArray());
        }

        public static string ToHexDataString(this BigInteger value)
        {
            return ToHexDataString(value.ToByteArray());
        }

        private static readonly DateTime unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        public static UInt32 ToUnixTime(this DateTime value)
        {
            return (UInt32)((value - unixEpoch).TotalSeconds);
        }

        public static DateTime UnixTimeToDateTime(this UInt32 value)
        {
            return unixEpoch.AddSeconds(value);
        }

        public static void Do(this SemaphoreSlim semaphore, Action action)
        {
            semaphore.Wait();
            try
            {
                action();
            }
            finally
            {
                semaphore.Release();
            }
        }

        public static T Do<T>(this SemaphoreSlim semaphore, Func<T> func)
        {
            semaphore.Wait();
            try
            {
                return func();
            }
            finally
            {
                semaphore.Release();
            }
        }

        public async static Task DoAsync(this SemaphoreSlim semaphore, Action action)
        {
            await semaphore.WaitAsync();
            try
            {
                action();
            }
            finally
            {
                semaphore.Release();
            }
        }

        public async static Task DoAsync(this SemaphoreSlim semaphore, Func<Task> action)
        {
            await semaphore.WaitAsync();
            try
            {
                await action();
            }
            finally
            {
                semaphore.Release();
            }
        }

        public static string StringJoin(this IEnumerable<string> enumerable, string separator)
        {
            return string.Join(separator, enumerable);
        }

        public static T RandomOrDefault<T>(this ImmutableList<T> array)
        {
            if (array.Count == 0)
                return default(T);

            return array[random.Next(array.Count)];
        }

        public static T RandomOrDefault<T>(this IList<T> list)
        {
            if (list.Count == 0)
                return default(T);

            return list[random.Next(list.Count)];
        }

        public static T RandomOrDefault2<T>(this IReadOnlyList<T> list)
        {
            if (list.Count == 0)
                return default(T);

            return list[random.Next(list.Count)];
        }

        public static string Format2(this string value, params object[] args)
        {
            var result = string.Format(value, args);

            var commentIndex = 0;
            while ((commentIndex = result.IndexOf("/*")) >= 0)
            {
                var commentEndIndex = result.IndexOf("*/", commentIndex + 2);
                if (commentEndIndex < 0)
                    break;

                result = result.Remove(commentIndex, commentEndIndex - commentIndex + 2);
            }

            return result;
        }

        public static int ToIntChecked(this UInt32 value)
        {
            checked
            {
                return (int)value;
            }
        }

        public static int ToIntChecked(this UInt64 value)
        {
            checked
            {
                return (int)value;
            }
        }

        public static int ToIntChecked(this long value)
        {
            checked
            {
                return (int)value;
            }
        }

        public static byte[] NextBytes(this Random random, long length)
        {
            var buffer = (byte[])Array.CreateInstance(typeof(byte), length);
            random.NextBytes(buffer);
            return buffer;
        }

        public static void Forget(this Task task)
        {
        }

        public static void DoRead(this ReaderWriterLockSlim rwLock, Action action)
        {
            rwLock.EnterReadLock();
            try
            {
                action();
            }
            finally
            {
                rwLock.ExitReadLock();
            }
        }

        public static T DoRead<T>(this ReaderWriterLockSlim rwLock, Func<T> func)
        {
            rwLock.EnterReadLock();
            try
            {
                return func();
            }
            finally
            {
                rwLock.ExitReadLock();
            }
        }

        public static void DoWrite(this ReaderWriterLockSlim rwLock, Action action)
        {
            rwLock.EnterWriteLock();
            try
            {
                action();
            }
            finally
            {
                rwLock.ExitWriteLock();
            }
        }

        public static T DoWrite<T>(this ReaderWriterLockSlim rwLock, Func<T> func)
        {
            rwLock.EnterWriteLock();
            try
            {
                return func();
            }
            finally
            {
                rwLock.ExitWriteLock();
            }
        }

        public static int THOUSAND(this int value)
        {
            return value * 1000;
        }

        public static int MILLION(this int value)
        {
            return value * 1000 * 1000;
        }

        public static int BILLION(this int value)
        {
            return value * 1000 * 1000 * 1000;
        }

        public static long THOUSAND(this long value)
        {
            return value * 1000;
        }

        public static long MILLION(this long value)
        {
            return value * 1000 * 1000;
        }

        public static long BILLION(this long value)
        {
            return value * 1000 * 1000 * 1000;
        }

        public static float ElapsedSecondsFloat(this Stopwatch stopwatch)
        {
            return (float)stopwatch.ElapsedTicks / Stopwatch.Frequency;
        }

        public static void DisposeList(this IEnumerable<IDisposable> disposables)
        {
            var exceptions = new List<Exception>();

            foreach (var item in disposables)
            {
                if (item != null)
                {
                    try
                    {
                        item.Dispose();
                    }
                    catch (Exception e)
                    {
                        exceptions.Add(e);
                    }
                }
            }

            if (exceptions.Count > 0)
                throw new AggregateException(exceptions);
        }

        public static Dictionary<TKey, TValue> ToDictionary<TKey, TValue>(this IEnumerable<KeyValuePair<TKey, TValue>> keyPairs)
        {
            return keyPairs.ToDictionary(x => x.Key, x => x.Value);
        }

        public static bool RemoveRange<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, IEnumerable<TKey> keys)
        {
            bool success = true;
            foreach (var key in keys.ToArray())
                success &= dictionary.Remove(key);

            return success;
        }

        public static List<T> SafeToList<T>(this IEnumerable<T> enumerable)
        {
            var list = new List<T>();
            foreach (var item in enumerable)
                list.Add(item);

            return list;
        }

        public static List<T> SafeToList<T>(this ICollection<T> collection)
        {
            var list = new List<T>(collection.Count);
            foreach (var item in collection)
                list.Add(item);

            return list;
        }

        public static byte[] HexToByteArray(this string value)
        {
            if (value.Length % 2 != 0)
                throw new ArgumentOutOfRangeException();

            var bytes = new byte[value.Length / 2];
            for (var i = 0; i < bytes.Length; i++)
            {
                bytes[i] = Byte.Parse(value.Substring(i * 2, 2), NumberStyles.HexNumber);
            }

            return bytes;
        }

        public static UInt32 NextUInt32(this Random random)
        {
            // purposefully left unchecked to get full range of UInt32
            return (UInt32)random.Next();
        }

        public static UInt64 NextUInt64(this Random random)
        {
            return (random.NextUInt32() << 32) + random.NextUInt32();
        }

        public static UInt256 NextUInt256(this Random random)
        {
            return new UInt256(
                (new BigInteger(random.NextUInt32()) << 96) +
                (new BigInteger(random.NextUInt32()) << 64) +
                (new BigInteger(random.NextUInt32()) << 32) +
                new BigInteger(random.NextUInt32()));
        }

        public static BigInteger NextUBigIntegerBytes(this Random random, int byteCount)
        {
            var bytes = random.NextBytes(byteCount).Concat(new byte[1]);
            return new BigInteger(bytes);
        }

        public static bool NextBool(this Random random)
        {
            return random.Next(2) == 0;
        }

        public static ImmutableBitArray ToImmutableBitArray(this BitArray bitArray)
        {
            return new ImmutableBitArray(bitArray);
        }

        public static void EnqueueRange<T>(this ConcurrentQueue<T> queue, IEnumerable<T> values)
        {
            foreach (var value in values)
                queue.Enqueue(value);
        }

        public static bool TryAddRange<T>(this IProducerConsumerCollection<T> collection, IEnumerable<T> values)
        {
            foreach (var value in values)
                if (!collection.TryAdd(value))
                    return false;

            return true;
        }

        public static BigInteger SumBigInteger<T>(this IEnumerable<T> values, Func<T, BigInteger> selector)
        {
            BigInteger sum = 0;
            foreach (var value in values)
                sum += selector(value);

            return sum;
        }

        public static IReadOnlyDictionary<TOuterKey, IReadOnlyDictionary<TInnerKey, TInnerValue>> AsReadOnly<TOuterKey, TInnerKey, TInnerValue>(this Dictionary<TOuterKey, Dictionary<TInnerKey, TInnerValue>> outerDictionary)
        {
            return new ReadOnlyDictionaryOfDictionary<TOuterKey, TInnerKey, TInnerValue>(outerDictionary);
        }

        public static void AddRange<T>(this ImmutableList<T>.Builder builder, IEnumerable<T> items)
        {
            builder.InsertRange(builder.Count, items);
        }

        public static void RemoveWhere<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, Func<KeyValuePair<TKey, TValue>, bool> predicate)
        {
            dictionary.RemoveRange(dictionary.Where(predicate).Select(x => x.Key));
        }

        public static void AddRange<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, IEnumerable<KeyValuePair<TKey, TValue>> keyPairs)
        {
            foreach (var keyPair in keyPairs)
                dictionary.Add(keyPair);
        }

        public static double? AverageOrDefault<TSource>(this IEnumerable<TSource> source, Func<TSource, long> selector)
        {
            try
            {
                return source.Average(selector);
            }
            catch (InvalidOperationException)
            {
                //TODO something better than catching exception?
                return null;
            }
        }

        public static byte[] ComputeDoubleHash(this SHA256Managed sha256, byte[] buffer)
        {
            return sha256.ComputeHash(sha256.ComputeHash(buffer));
        }
    }
}
