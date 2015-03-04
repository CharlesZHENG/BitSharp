using BitSharp.Common.ExtensionMethods;
using NLog;
using System;
using System.Collections.Generic;
using System.IO;

namespace BitSharp.Core.Domain
{
    public class UtxoStream : Stream
    {
        private readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly IEnumerator<UnspentTx> unspentTransactions;
        private readonly List<byte> unreadBytes;
        private int totalBytes;

        public UtxoStream(IEnumerable<UnspentTx> unspentTransactions)
        {
            this.unspentTransactions = unspentTransactions.GetEnumerator();
            this.unreadBytes = new List<byte>();
        }

        protected override void Dispose(bool disposing)
        {
            this.logger.Info("UTXO Commitment Bytes: {0:#,##0}".Format2(this.totalBytes));

            this.unspentTransactions.Dispose();
            base.Dispose(disposing);
        }

        public override bool CanRead
        {
            get { return true; }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanWrite
        {
            get { return false; }
        }

        public override void Flush()
        {
        }

        public override long Length
        {
            get { throw new NotSupportedException(); }
        }

        public override long Position
        {
            get
            {
                throw new NotSupportedException();
            }
            set
            {
                throw new NotSupportedException();
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            while (
                unreadBytes.Count < count
                && this.unspentTransactions.MoveNext()
            )
            {
                var unspentTx = this.unspentTransactions.Current;
                unreadBytes.AddRange(DataEncoder.EncodeUnspentTx(unspentTx));
            }

            var available = unreadBytes.Count;
            if (available >= count)
            {
                unreadBytes.CopyTo(0, buffer, offset, count);
                unreadBytes.RemoveRange(0, count);
                this.totalBytes += count;
                return count;
            }
            else
            {
                unreadBytes.CopyTo(0, buffer, offset, available);
                unreadBytes.Clear();
                this.totalBytes += available;
                return available;
            }
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }
    }
}
