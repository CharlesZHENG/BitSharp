using BitSharp.Common;
using BitSharp.Core.Rules;
using BitSharp.Node.Network;
using NLog;
using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace BitSharp.Node.Workers
{
    internal class ListenWorker : Worker
    {
        private static readonly int SERVER_BACKLOG = 10;

        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly LocalClient localClient;
        private readonly PeerWorker peerWorker;

        private Socket listenSocket;

        public ListenWorker(LocalClient localClient, PeerWorker peerWorker)
            : base("ListenWorker", initialNotify: true, minIdleTime: TimeSpan.Zero, maxIdleTime: TimeSpan.Zero)
        {
            this.localClient = localClient;
            this.peerWorker = peerWorker;
        }

        protected override void SubDispose()
        {
            DisposeSocket();
        }

        protected override void SubStart()
        {
            DisposeSocket();

            try
            {
                switch (this.localClient.Type)
                {
                    case ChainType.MainNet:
                    case ChainType.TestNet3:
                        var externalIPAddress = Messaging.GetExternalIPAddress();
                        var localhost = Dns.GetHostEntry(Dns.GetHostName());

                        this.listenSocket = new Socket(externalIPAddress.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                        this.listenSocket.Bind(new IPEndPoint(localhost.AddressList.Where(x => x.AddressFamily == externalIPAddress.AddressFamily).First(), Messaging.Port));
                        break;

                    case ChainType.Regtest:
                        this.listenSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                        this.listenSocket.Bind(new IPEndPoint(IPAddress.Parse("127.0.0.1"), Messaging.Port));
                        break;
                }
                this.listenSocket.Listen(SERVER_BACKLOG);
            }
            catch (Exception ex)
            {
                logger.Error(ex, "Failed to start listener socket.");
                DisposeSocket();
                throw;
            }
        }

        protected override void SubStop()
        {
            listenSocket?.Close();
            //DisposeSocket();
        }

        protected override async Task WorkAction()
        {
            try
            {
                var newSocket = await Task.Factory.FromAsync<Socket>(this.listenSocket.BeginAccept(null, null), this.listenSocket.EndAccept);
                await this.peerWorker.AddIncomingPeer(newSocket);
            }
            catch (Exception ex) when (!(ex is OperationCanceledException))
            {
                logger.Warn(ex, "Failed incoming connection.");
            }
        }

        private void DisposeSocket()
        {
            this.listenSocket?.Dispose();
            this.listenSocket = null;
        }
    }
}
