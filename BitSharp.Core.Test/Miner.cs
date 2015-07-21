using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Domain;
using NLog;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Core.Test
{
    public class Miner
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private class LocalMinerState
        {
            public readonly byte[] headerBytes;
            public long total;

            public LocalMinerState(byte[] headerBytes)
            {
                this.headerBytes = (byte[])headerBytes.Clone();
                this.total = 0;
            }
        }

        public BlockHeader MineBlockHeader(BlockHeader blockHeader, UInt256 hashTarget)
        {
            var blockHeaderBytes = DataEncoder.EncodeBlockHeader(blockHeader);

            var hashTargetBytes = hashTarget.ToByteArray();

            var start = 0;
            var finish = UInt32.MaxValue;
            var total = 0L;
            var nonceIndex = 76;
            var minedNonce = (UInt32?)null;

            logger.Debug($"Starting mining: {DateTime.Now:hh':'mm':'ss}");

            var stopwatch = Stopwatch.StartNew();

            Parallel.For(
                start, finish,
                new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount * 2 },
                () => new LocalMinerState(blockHeaderBytes),
                (nonceLong, loopState, localState) =>
                {
                    localState.total++;

                    var nonce = (UInt32)nonceLong;
                    var nonceBytes = Bits.GetBytes(nonce);
                    Buffer.BlockCopy(nonceBytes, 0, localState.headerBytes, nonceIndex, 4);

                    var headerBytes = localState.headerBytes;
                    var hashBytes = SHA256Static.ComputeDoubleHash(headerBytes);

                    if (BytesCompareLE(hashBytes, hashTargetBytes) < 0)
                    {
                        minedNonce = nonce;
                        loopState.Stop();
                    }

                    return localState;
                },
                localState => { Interlocked.Add(ref total, localState.total); });

            stopwatch.Stop();

            var hashRate = ((float)total / 1000 / 1000) / ((float)stopwatch.ElapsedMilliseconds / 1000);

            if (minedNonce != null)
            {
                logger.Debug($"Found block in {stopwatch.Elapsed:hh':'mm':'ss} hh:mm:ss at Nonce {minedNonce}, Hash Rate: {hashRate} mHash/s, Total Hash Attempts: {total:N0}, Found Hash: {blockHeader.With(Nonce: minedNonce).Hash}");
                return blockHeader.With(Nonce: minedNonce);
            }
            else
            {
                logger.Debug($"No block found in {stopwatch.Elapsed:hh':'mm':'ss} hh:mm:ss, Hash Rate: {hashRate} mHash/s, Total Hash Attempts: {total:N0}, Found Hash: {blockHeader.With(Nonce: minedNonce).Hash}");
                return null;
            }
        }

        private static int BytesCompareLE(byte[] a, byte[] b)
        {
            if (a.Length != b.Length)
                throw new ArgumentException();

            for (var i = a.Length - 1; i >= 0; i--)
            {
                if (a[i] < b[i])
                    return -1;
                else if (a[i] > b[i])
                    return +1;
            }

            return 0;
        }
    }
}
