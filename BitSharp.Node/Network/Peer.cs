using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Node.Domain;
using NLog;
using System;
using System.Collections.Immutable;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Node.Network
{
    public class Peer : IDisposable
    {
        public event Action<Peer, GetBlocksPayload> OnGetBlocks;
        public event Action<Peer, GetBlocksPayload> OnGetHeaders;
        public event Action<Peer, InventoryPayload> OnGetData;
        public event Action<Peer, ImmutableArray<byte>> OnPing;
        public event Action<Peer> OnDisconnect;

        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly object objectLock = new object();
        private readonly SemaphoreSlim semaphore = new SemaphoreSlim(1);
        private bool isDisposed;

        private bool startedConnecting = false;
        private bool isConnected = false;
        private /*readonly*/ IPEndPoint localEndPoint;
        private readonly IPEndPoint remoteEndPoint;
        private readonly Socket socket;
        private readonly RemoteReceiver receiver;
        private readonly RemoteSender sender;
        private readonly bool isSeed;

        private CountMeasure blockMissCountMeasure;

        public Peer(IPEndPoint remoteEndPoint, bool isSeed)
        {
            this.remoteEndPoint = remoteEndPoint;
            this.isSeed = isSeed;

            this.socket = new Socket(remoteEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            this.receiver = new RemoteReceiver(this, this.socket, persistent: false);
            this.sender = new RemoteSender(this.socket);

            this.blockMissCountMeasure = new CountMeasure(TimeSpan.FromMinutes(10));

            WireNode();
        }

        public Peer(Socket socket, bool isSeed)
        {
            this.socket = socket;
            this.isConnected = true;

            this.localEndPoint = (IPEndPoint)socket.LocalEndPoint;
            this.remoteEndPoint = (IPEndPoint)socket.RemoteEndPoint;

            this.receiver = new RemoteReceiver(this, this.socket, persistent: false);
            this.sender = new RemoteSender(this.socket);

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

                    this.sender.Dispose();
                    this.socket.Dispose();
                    this.blockMissCountMeasure.Dispose();
                    this.semaphore.Dispose();

                    this.isDisposed = true;
                }
            }
        }

        public IPEndPoint LocalEndPoint { get { return this.localEndPoint; } }

        public IPEndPoint RemoteEndPoint { get { return this.remoteEndPoint; } }

        public RemoteReceiver Receiver { get { return this.receiver; } }

        public RemoteSender Sender { get { return this.sender; } }

        public bool IsConnected { get { return this.isConnected; } }

        public bool IsSeed { get { return this.isSeed; } }

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
                if (this.isConnected || this.startedConnecting)
                    return;

                // indicate that connecting will be started
                this.startedConnecting = true;
            }

            // start the connection
            try
            {
                await Task.Factory.FromAsync(this.socket.BeginConnect(this.remoteEndPoint, null, null), this.socket.EndConnect);

                this.localEndPoint = (IPEndPoint)this.socket.LocalEndPoint;
                this.isConnected = true;
            }
            catch (Exception e)
            {
                logger.Debug(string.Format("Error on connecting to {0}", remoteEndPoint), e);
                Disconnect();
            }
            finally
            {
                // ensure started connecting flag is cleared
                this.startedConnecting = false;
            }
        }

        public void Disconnect()
        {
            var handler = this.OnDisconnect;
            if (handler != null)
                handler(this);

            this.Dispose();
        }

        private void WireNode()
        {
            this.receiver.OnFailed += HandleFailed;
            this.sender.OnFailed += HandleFailed;
            this.receiver.OnGetBlocks += HandleGetBlocks;
            this.receiver.OnGetHeaders += HandleGetHeaders;
            this.receiver.OnGetData += HandleGetData;
            this.receiver.OnPing += HandlePing;
        }

        private void UnwireNode()
        {
            this.receiver.OnFailed -= HandleFailed;
            this.sender.OnFailed -= HandleFailed;
            this.receiver.OnGetBlocks -= HandleGetBlocks;
            this.receiver.OnGetHeaders -= HandleGetHeaders;
            this.receiver.OnGetData -= HandleGetData;
            this.receiver.OnPing -= HandlePing;
        }

        private void HandleFailed(Exception e)
        {
            if (e != null)
                logger.Debug("Remote peer failed: {0}".Format2(this.remoteEndPoint), e);
            else
                logger.Debug("Remote peer failed: {0}".Format2(this.remoteEndPoint));

            Disconnect();
        }

        private void HandleGetBlocks(GetBlocksPayload payload)
        {
            var handler = this.OnGetBlocks;
            if (handler != null)
                handler(this, payload);
        }

        private void HandleGetHeaders(GetBlocksPayload payload)
        {
            var handler = this.OnGetHeaders;
            if (handler != null)
                handler(this, payload);
        }

        private void HandleGetData(InventoryPayload payload)
        {
            var handler = this.OnGetData;
            if (handler != null)
                handler(this, payload);
        }

        private void HandlePing(ImmutableArray<byte> payload)
        {
            var handler = this.OnPing;
            if (handler != null)
                handler(this, payload);
        }
    }
}
