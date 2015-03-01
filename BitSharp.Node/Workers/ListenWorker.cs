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

        private readonly Logger logger;
        private readonly LocalClient localClient;
        private readonly PeerWorker peerWorker;

        private Socket listenSocket;

        public ListenWorker(Logger logger, LocalClient localClient, PeerWorker peerWorker)
            : base("ListenWorker", initialNotify: true, minIdleTime: TimeSpan.Zero, maxIdleTime: TimeSpan.Zero, logger: logger)
        {
            this.logger = logger;
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
                    case RulesEnum.MainNet:
                    case RulesEnum.TestNet3:
                        var externalIPAddress = Messaging.GetExternalIPAddress();
                        var localhost = Dns.GetHostEntry(Dns.GetHostName());

                        this.listenSocket = new Socket(externalIPAddress.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                        this.listenSocket.Bind(new IPEndPoint(localhost.AddressList.Where(x => x.AddressFamily == externalIPAddress.AddressFamily).First(), Messaging.Port));
                        break;

                    case RulesEnum.ComparisonToolTestNet:
                        this.listenSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                        this.listenSocket.Bind(new IPEndPoint(IPAddress.Parse("127.0.0.1"), Messaging.Port));
                        break;
                }
                this.listenSocket.Listen(SERVER_BACKLOG);
            }
            catch (Exception e)
            {
                this.logger.Error("Failed to start listener socket.", e);
                DisposeSocket();
                throw;
            }
        }

        protected override void SubStop()
        {
            //DisposeSocket();
        }

        protected override void WorkAction()
        {
            try
            {
                var newSocketTask = Task.Factory.FromAsync<Socket>(this.listenSocket.BeginAccept(null, null), this.listenSocket.EndAccept);

                while (!(newSocketTask.IsCanceled || newSocketTask.IsFaulted || newSocketTask.IsCompleted))
                {
                    // cooperative loop
                    this.ThrowIfCancelled();

                    newSocketTask.Wait(TimeSpan.FromMilliseconds(200));
                }

                var newSocket = newSocketTask.Result;

                this.peerWorker.AddIncomingPeer(newSocket);
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception e)
            {
                this.logger.Warn("Failed incoming connection.", e);
            }
        }

        private void DisposeSocket()
        {
            if (this.listenSocket != null)
            {
                this.listenSocket.Dispose();
                this.listenSocket = null;
            }
        }
    }
}
