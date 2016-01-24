using BitSharp.Common;
using NLog;
using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Network
{
    public class Peer : IDisposable
    {
        public event Action<Peer, Exception> OnDisconnect;

        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly object objectLock = new object();
        private readonly SemaphoreSlim semaphore = new SemaphoreSlim(1);
        private bool isDisposed;

        private bool startedConnecting = false;
        private readonly Socket socket;

        private CountMeasure blockMissCountMeasure;

        public Peer(IPEndPoint remoteEndPoint, bool isSeed, bool isIncoming)
        {
            RemoteEndPoint = remoteEndPoint;
            IsSeed = isSeed;
            IsIncoming = isIncoming;

            this.socket = new Socket(remoteEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            Receiver = new RemoteReceiver(this, this.socket);
            Sender = new RemoteSender(this, this.socket);

            this.blockMissCountMeasure = new CountMeasure(TimeSpan.FromMinutes(10));

            WireNode();
        }

        public Peer(Socket socket, bool isSeed, bool isIncoming)
        {
            this.socket = socket;
            this.IsConnected = true;
            IsIncoming = isIncoming;

            LocalEndPoint = (IPEndPoint)socket.LocalEndPoint;
            RemoteEndPoint = (IPEndPoint)socket.RemoteEndPoint;

            Receiver = new RemoteReceiver(this, this.socket);
            Sender = new RemoteSender(this, this.socket);

            this.blockMissCountMeasure = new CountMeasure(TimeSpan.FromMinutes(10));

            WireNode();
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
                lock (this.objectLock)
                {
                    if (this.isDisposed)
                        return;

                    UnwireNode();

                    this.Sender.Dispose();
                    this.socket.Dispose();
                    this.blockMissCountMeasure.Dispose();
                    this.semaphore.Dispose();

                    this.IsConnected = false;

                    this.isDisposed = true;
                }
            }
        }

        public IPEndPoint LocalEndPoint { get; private set; }

        public IPEndPoint RemoteEndPoint { get; }

        public RemoteReceiver Receiver { get; }

        public RemoteSender Sender { get; }

        public bool IsConnected { get; private set; }

        public bool IsSeed { get; }

        public bool IsIncoming { get; }

        public int BlockMissCount
        {
            get
            {
                lock (this.objectLock)
                {
                    if (!this.isDisposed)
                        return this.blockMissCountMeasure.GetCount();
                    else
                        return 0;
                }
            }
        }

        public void AddBlockMiss()
        {
            lock (this.objectLock)
            {
                if (!this.isDisposed)
                    this.blockMissCountMeasure.Tick();
            }
        }

        public async Task ConnectAsync()
        {
            // take the lock to see if a connect can be started
            lock (this.objectLock)
            {
                if (this.isDisposed)
                    return;

                // don't connect if already connected, or started connecting elsewhere
                if (this.IsConnected || this.startedConnecting)
                    return;

                // indicate that connecting will be started
                this.startedConnecting = true;
            }

            // start the connection
            try
            {
                await Task.Factory.FromAsync(this.socket.BeginConnect(this.RemoteEndPoint, null, null), this.socket.EndConnect);

                this.LocalEndPoint = (IPEndPoint)this.socket.LocalEndPoint;
                this.IsConnected = true;
            }
            catch (Exception ex)
            {
                logger.Debug(ex, $"Error on connecting to {RemoteEndPoint}");
                Disconnect(ex);

                throw;
            }
            finally
            {
                // ensure started connecting flag is cleared
                this.startedConnecting = false;
            }
        }

        public void Disconnect(Exception ex = null)
        {
            this.OnDisconnect?.Invoke(this, ex);

            this.Dispose();
        }

        private void WireNode()
        {
            this.Receiver.OnFailed += HandleFailed;
            this.Sender.OnFailed += HandleFailed;
        }

        private void UnwireNode()
        {
            this.Receiver.OnFailed -= HandleFailed;
            this.Sender.OnFailed -= HandleFailed;
        }

        private void HandleFailed(Peer peer, Exception ex)
        {
            if (ex != null)
                logger.Debug(ex, $"Remote peer failed: {this.RemoteEndPoint}");
            else
                logger.Debug($"Remote peer failed: {this.RemoteEndPoint}");

            Disconnect(ex);
        }
    }
}
