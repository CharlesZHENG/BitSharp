using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using BitSharp.Node.Domain;
using BitSharp.Node.ExtensionMethods;
using BitSharp.Node.Network;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Node.Workers
{
    internal class PeerWorker : Worker
    {
        private static readonly int CONNECTED_MAX = 3;
        private static readonly int PENDING_MAX = 2 * CONNECTED_MAX;
        private static readonly int HANDSHAKE_TIMEOUT_MS = 15000;

        private readonly Random random = new Random();
        private readonly Logger logger;
        private readonly LocalClient localClient;
        private readonly CoreDaemon coreDaemon;

        private readonly SortedValueDictionary<IPEndPoint, CandidatePeer> unconnectedPeers = new SortedValueDictionary<IPEndPoint, CandidatePeer>();
        private readonly SemaphoreSlim unconnectedPeersLock = new SemaphoreSlim(1);
        private readonly ConcurrentSet<IPEndPoint> badPeers = new ConcurrentSet<IPEndPoint>();
        private readonly ConcurrentSet<Peer> pendingPeers = new ConcurrentSet<Peer>();
        private readonly ConcurrentSet<Peer> connectedPeers = new ConcurrentSet<Peer>();

        private int incomingCount;

        public PeerWorker(WorkerConfig workerConfig, Logger logger, LocalClient localClient, CoreDaemon coreDaemon)
            : base("PeerWorker", workerConfig.initialNotify, workerConfig.minIdleTime, workerConfig.maxIdleTime, logger)
        {
            this.logger = logger;
            this.localClient = localClient;
            this.coreDaemon = coreDaemon;
        }

        public event Action<Peer> PeerConnected;

        public event Action<Peer> PeerDisconnected;

        internal int UnconnectedPeersCount
        {
            get
            {
                return this.unconnectedPeersLock.Do(() =>
                    this.unconnectedPeers.Count);
            }
        }

        internal ConcurrentSet<IPEndPoint> BadPeers { get { return this.badPeers; } }

        internal ConcurrentSet<Peer> PendingPeers { get { return this.pendingPeers; } }

        internal ConcurrentSet<Peer> ConnectedPeers { get { return this.connectedPeers; } }

        internal int IncomingCount { get { return this.incomingCount; } }

        public void AddCandidatePeer(CandidatePeer peer)
        {
            if (this.badPeers.Contains(peer.IPEndPoint))
                return;

            this.unconnectedPeersLock.Do(() =>
                this.unconnectedPeers.Add(peer.IPEndPoint, peer));
        }

        public void AddIncomingPeer(Socket socket)
        {
            var peer = new Peer(socket, isSeed: false, logger: this.logger);
            try
            {
                ConnectAndHandshake(peer, isIncoming: true)
                    .ContinueWith(task => DisconnectPeer(peer, task.Exception), TaskContinuationOptions.OnlyOnFaulted)
                    .Forget();
            }
            catch (Exception e)
            {
                DisconnectPeer(peer, e);
                throw;
            }
        }

        //TODO if a peer is in use on another thread, it could get disconnected here
        //TODO e.g. slow peers are detected and disconnected separately from the block requester using them
        public void DisconnectPeer(Peer peer)
        {
            DisconnectPeer(peer, null);
        }

        public void DisconnectPeer(Peer peer, Exception e)
        {
            if (e != null)
                this.logger.Debug("Remote peer failed: {0}".Format2(peer.RemoteEndPoint), e);

            RaisePeerDisconnected(peer);

            this.badPeers.Add(peer.RemoteEndPoint); //TODO

            this.unconnectedPeersLock.Do(() =>
                this.unconnectedPeers.Remove(peer.RemoteEndPoint));
            this.pendingPeers.TryRemove(peer);
            this.connectedPeers.TryRemove(peer);

            peer.OnDisconnect -= DisconnectPeer;
            peer.Dispose();
        }

        protected override void SubDispose()
        {
            this.pendingPeers.DisposeList();
            this.connectedPeers.DisposeList();
        }

        protected override void SubStart()
        {
        }

        protected override void SubStop()
        {
        }

        protected override void WorkAction()
        {
            if (this.localClient.Type == RulesEnum.ComparisonToolTestNet)
                return;

            foreach (var peer in this.connectedPeers)
            {
                // clear out any disconnected peers
                if (!peer.IsConnected)
                    DisconnectPeer(peer);

                if (this.connectedPeers.Count <= 5)
                    break;

                // disconnect seed peers, once enough peers are connected
                if (peer.IsSeed)
                    DisconnectPeer(peer, null);

                // disconnect slow peers
                if (peer.BlockMissCount >= 5)
                {
                    this.logger.Info("Disconnecting slow peer: {0}".Format2(peer.RemoteEndPoint));
                    DisconnectPeer(peer, null);
                }
            }

            // get peer counts
            var connectedCount = this.connectedPeers.Count;
            var pendingCount = this.pendingPeers.Count;
            var maxConnections = CONNECTED_MAX; // Math.Max(CONNECTED_MAX + 20, PENDING_MAX);

            // if there aren't enough peers connected and there is a pending connection slot available, make another connection
            if (connectedCount < CONNECTED_MAX
                 && pendingCount < PENDING_MAX
                 && (connectedCount + pendingCount) < maxConnections)
            {
                // get number of connections to attempt
                var connectCount = maxConnections - (connectedCount + pendingCount);

                // take a selection of unconnected peers, ordered by time
                var unconnectedPeersLocal = this.unconnectedPeersLock.Do(() =>
                    this.unconnectedPeers.Values.Take(connectCount).ToArray());

                var connectTasks = new List<Task>();
                foreach (var candidatePeer in unconnectedPeersLocal)
                {
                    // cooperative loop
                    this.ThrowIfCancelled();

                    // connect to peer
                    connectTasks.Add(ConnectToPeer(candidatePeer.IPEndPoint, candidatePeer.IsSeed));
                }

                // wait for pending connection attempts to complete
                //Task.WaitAll(connectTasks.ToArray(), this.shutdownToken.Token);
            }

            // check if there are too many peers connected
            var overConnected = this.connectedPeers.Count - CONNECTED_MAX;
            if (overConnected > 0)
            {
                foreach (var peer in this.connectedPeers.Take(overConnected))
                {
                    // cooperative loop
                    this.ThrowIfCancelled();

                    this.logger.Debug("Too many peers connected ({0}), disconnecting {1}".Format2(overConnected, peer));
                    DisconnectPeer(peer, null);
                }
            }
        }

        private async Task<Peer> ConnectToPeer(IPEndPoint remoteEndPoint, bool isSeed)
        {
            try
            {
                var peer = new Peer(remoteEndPoint, isSeed, this.logger);
                try
                {
                    this.unconnectedPeersLock.Do(() =>
                        this.unconnectedPeers.Remove(remoteEndPoint));
                    this.pendingPeers.TryAdd(peer);

                    await ConnectAndHandshake(peer, isIncoming: false);
                    await PeerStartup(peer);

                    return peer;
                }
                catch (Exception e)
                {
                    this.logger.Debug("Could not connect to {0}".Format2(remoteEndPoint), e);
                    DisconnectPeer(peer, e);
                    return null;
                }
            }
            catch (Exception)
            {
                this.badPeers.Add(remoteEndPoint); //TODO
                this.unconnectedPeersLock.Do(() =>
                    this.unconnectedPeers.Remove(remoteEndPoint));
                throw;
            }
        }

        private async Task ConnectAndHandshake(Peer peer, bool isIncoming)
        {
            // connect
            await peer.ConnectAsync();
            if (!peer.IsConnected)
                throw new Exception();

            // setup task to wait for verack
            var verAckTask = peer.Receiver.WaitForMessage(x => x.Command == "verack", HANDSHAKE_TIMEOUT_MS);

            // setup task to wait for version
            var versionTask = peer.Receiver.WaitForMessage(x => x.Command == "version", HANDSHAKE_TIMEOUT_MS);

            // start listening for messages after tasks have been setup
            peer.Receiver.Listen();

            // send our local version
            var nodeId = random.NextUInt64(); //TODO should be generated and verified on version message

            var currentHeight = this.coreDaemon.CurrentChain.Height;
            await peer.Sender.SendVersion(Messaging.GetExternalIPEndPoint(), peer.RemoteEndPoint, nodeId, (UInt32)currentHeight);

            // wait for our local version to be acknowledged by the remote peer
            // wait for remote peer to send their version
            await Task.WhenAll(verAckTask, versionTask);

            //TODO shouldn't have to decode again
            var versionMessage = versionTask.Result;
            var versionPayload = NodeEncoder.DecodeVersionPayload(versionMessage.Payload.ToArray(), versionMessage.Payload.Length);

            var remoteAddressWithTime = new NetworkAddressWithTime
            (
                Time: DateTime.UtcNow.ToUnixTime(),
                NetworkAddress: new NetworkAddress
                (
                    Services: versionPayload.LocalAddress.Services,
                    IPv6Address: versionPayload.LocalAddress.IPv6Address,
                    Port: versionPayload.LocalAddress.Port
                )
            );

            // acknowledge their version
            await peer.Sender.SendVersionAcknowledge();

            if (isIncoming)
                Interlocked.Increment(ref this.incomingCount);

            this.pendingPeers.TryRemove(peer);
            this.connectedPeers.TryAdd(peer);

            peer.OnDisconnect += DisconnectPeer;
            RaisePeerConnected(peer);
        }

        private async Task PeerStartup(Peer peer)
        {
            await peer.Sender.RequestKnownAddressesAsync();
        }

        private void RaisePeerConnected(Peer peer)
        {
            var handler = this.PeerConnected;
            if (handler != null)
                handler(peer);
        }

        private void RaisePeerDisconnected(Peer peer)
        {
            var handler = this.PeerDisconnected;
            if (handler != null)
                handler(peer);
        }
    }
}
