﻿using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core;
using BitSharp.Core.Domain;
using BitSharp.Core.Rules;
using BitSharp.Core.Storage;
using BitSharp.Node.Domain;
using BitSharp.Node.ExtensionMethods;
using BitSharp.Node.Network;
using BitSharp.Node.Storage;
using BitSharp.Node.Workers;
using Ninject;
using NLog;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Node
{
    public class LocalClient : IDisposable
    {
        public event Action<Peer, Block> OnBlock;
        public event Action<Peer, IImmutableList<BlockHeader>> OnBlockHeaders;

        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly CancellationTokenSource shutdownToken;
        private readonly Random random = new Random();

        private readonly RulesEnum type;
        private readonly IKernel kernel;
        private readonly IBlockchainRules rules;
        private readonly CoreDaemon coreDaemon;
        private readonly CoreStorage coreStorage;
        private readonly INetworkPeerStorage networkPeerStorage;

        private readonly PeerWorker peerWorker;
        private readonly ListenWorker listenWorker;
        private readonly HeadersRequestWorker headersRequestWorker;
        private readonly BlockRequestWorker blockRequestWorker;
        private readonly WorkerMethod statsWorker;

        private RateMeasure messageRateMeasure;

        private bool isDisposed;

        public LocalClient(RulesEnum type, IKernel kernel, IBlockchainRules rules, CoreDaemon coreDaemon, INetworkPeerStorage networkPeerStorage)
        {
            this.shutdownToken = new CancellationTokenSource();

            this.type = type;
            this.kernel = kernel;
            this.rules = rules;
            this.coreDaemon = coreDaemon;
            this.coreStorage = coreDaemon.CoreStorage;
            this.networkPeerStorage = networkPeerStorage;

            this.messageRateMeasure = new RateMeasure();

            this.peerWorker = new PeerWorker(
                new WorkerConfig(initialNotify: true, minIdleTime: TimeSpan.FromSeconds(1), maxIdleTime: TimeSpan.FromSeconds(1)),
                this, this.coreDaemon);

            this.listenWorker = new ListenWorker(this, this.peerWorker);

            this.headersRequestWorker = new HeadersRequestWorker(
                new WorkerConfig(initialNotify: true, minIdleTime: TimeSpan.FromMilliseconds(50), maxIdleTime: TimeSpan.FromSeconds(5)),
                this, this.coreDaemon);

            this.blockRequestWorker = new BlockRequestWorker(
                new WorkerConfig(initialNotify: true, minIdleTime: TimeSpan.FromMilliseconds(50), maxIdleTime: TimeSpan.FromSeconds(30)),
                this, this.coreDaemon);

            this.statsWorker = new WorkerMethod("LocalClient.StatsWorker", StatsWorker, true, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5));

            this.peerWorker.PeerConnected += HandlePeerConnected;
            this.peerWorker.PeerDisconnected += HandlePeerDisconnected;

            switch (this.Type)
            {
                case RulesEnum.MainNet:
                    Messaging.Port = 8333;
                    Messaging.Magic = Messaging.MAGIC_MAIN;
                    break;

                case RulesEnum.TestNet3:
                    Messaging.Port = 18333;
                    Messaging.Magic = Messaging.MAGIC_TESTNET3;
                    break;

                case RulesEnum.ComparisonToolTestNet:
                    Messaging.Port = 18444;
                    Messaging.Magic = Messaging.MAGIC_COMPARISON_TOOL;
                    break;
            }
        }

        public RulesEnum Type { get { return this.type; } }

        internal ConcurrentSet<Peer> ConnectedPeers { get { return this.peerWorker.ConnectedPeers; } }

        public void Start(bool connectToPeers = true)
        {
            if (this.Type != RulesEnum.ComparisonToolTestNet)
            {
                this.headersRequestWorker.Start();
            }
            this.blockRequestWorker.Start();

            this.statsWorker.Start();

            if (connectToPeers)
            {
                this.peerWorker.Start();
                if (this.Type != RulesEnum.ComparisonToolTestNet)
                {
                    // add seed peers
                    Task.Run(() => AddSeedPeers());

                    // add known peers
                    Task.Run(() => AddKnownPeers());
                }
                else
                {
                    Messaging.GetExternalIPEndPoint();
                    this.listenWorker.Start();
                }
            }
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
                this.shutdownToken.Cancel();

                this.peerWorker.PeerConnected -= HandlePeerConnected;
                this.peerWorker.PeerDisconnected -= HandlePeerDisconnected;

                this.messageRateMeasure.Dispose();
                this.statsWorker.Dispose();
                this.headersRequestWorker.Dispose();
                this.blockRequestWorker.Dispose();
                this.peerWorker.Dispose();
                this.listenWorker.Dispose();
                this.shutdownToken.Dispose();

                isDisposed = true;
            }
        }

        public float GetBlockDownloadRate(TimeSpan? perUnitTime = null)
        {
            return this.blockRequestWorker.GetBlockDownloadRate(perUnitTime);
        }

        public int GetDuplicateBlockDownloadCount()
        {
            return this.blockRequestWorker.GetDuplicateBlockDownloadCount();
        }

        public int GetBlockMissCount()
        {
            return this.coreDaemon.GetBlockMissCount();
        }

        internal void DiscouragePeer(IPEndPoint peerEndPoint)
        {
            // discourage a peer by reducing their last seen time
            NetworkAddressWithTime address;
            if (this.networkPeerStorage.TryGetValue(peerEndPoint.ToNetworkAddressKey(), out address))
            {
                var newTime = (address.Time.UnixTimeToDateTime() - TimeSpan.FromDays(7)).ToUnixTime();
                this.networkPeerStorage[address.NetworkAddress.GetKey()] = address.With(Time: newTime);
            }
        }

        private void AddSeedPeers()
        {
            Action<string> addSeed =
                hostNameOrAddress =>
                {
                    try
                    {
                        IPAddress ipAddress;
                        if (!IPAddress.TryParse(hostNameOrAddress, out ipAddress))
                            ipAddress = Dns.GetHostEntry(hostNameOrAddress).AddressList.First();

                        this.peerWorker.AddCandidatePeer(
                            new CandidatePeer
                            (
                                ipEndPoint: new IPEndPoint(ipAddress, Messaging.Port),
                                time: DateTime.MinValue,
                                isSeed: this.Type == RulesEnum.MainNet ? true : false
                            ));
                    }
                    catch (SocketException ex)
                    {
                        logger.Warn(ex, "Failed to add seed peer {0}".Format2(hostNameOrAddress));
                    }
                };

            switch (this.Type)
            {
                case RulesEnum.MainNet:
                    addSeed("seed.bitcoin.sipa.be");
                    addSeed("dnsseed.bluematt.me");
                    //addSeed("dnsseed.bitcoin.dashjr.org");
                    addSeed("seed.bitcoinstats.com");
                    //addSeed("seed.bitnodes.io");
                    //addSeed("seeds.bitcoin.open-nodes.org");
                    addSeed("bitseed.xf2.org");
                    break;

                case RulesEnum.TestNet3:
                    addSeed("testnet-seed.alexykot.me");
                    addSeed("testnet-seed.bitcoin.petertodd.org");
                    addSeed("testnet-seed.bluematt.me");
                    break;
            }
        }

        private void AddKnownPeers()
        {
            var count = 0;
            foreach (var knownAddress in this.networkPeerStorage.Values)
            {
                this.peerWorker.AddCandidatePeer(
                    new CandidatePeer
                    (
                        ipEndPoint: knownAddress.NetworkAddress.ToIPEndPoint(),
                        time: knownAddress.Time.UnixTimeToDateTime() + TimeSpan.FromDays(random.NextDouble(-2, +2)),
                        isSeed: false
                    ));
                count++;
            }

            logger.Info("LocalClients loaded {0} known peers from database".Format2(count));
        }

        private void HandlePeerConnected(Peer peer)
        {
            var remoteAddressWithTime = new NetworkAddressWithTime(DateTime.UtcNow.ToUnixTime(), peer.RemoteEndPoint.ToNetworkAddress(/*TODO*/services: 0));
            this.networkPeerStorage[remoteAddressWithTime.NetworkAddress.GetKey()] = remoteAddressWithTime;

            WirePeerEvents(peer);

            this.statsWorker.NotifyWork();
            this.headersRequestWorker.NotifyWork();
            this.blockRequestWorker.NotifyWork();
        }

        private void HandlePeerDisconnected(Peer peer)
        {
            UnwirePeerEvents(peer);

            this.statsWorker.NotifyWork();
            this.headersRequestWorker.NotifyWork();
            this.blockRequestWorker.NotifyWork();
        }

        private void WirePeerEvents(Peer peer)
        {
            peer.Receiver.OnMessage += OnMessage;
            peer.Receiver.OnInventoryVectors += OnInventoryVectors;
            peer.Receiver.OnBlock += HandleBlock;
            peer.Receiver.OnBlockHeaders += HandleBlockHeaders;
            peer.Receiver.OnTransaction += OnTransaction;
            peer.Receiver.OnReceivedAddresses += OnReceivedAddresses;
            peer.OnGetBlocks += OnGetBlocks;
            peer.OnGetHeaders += OnGetHeaders;
            peer.OnGetData += OnGetData;
            peer.OnPing += OnPing;
        }

        private void UnwirePeerEvents(Peer peer)
        {
            peer.Receiver.OnMessage -= OnMessage;
            peer.Receiver.OnInventoryVectors -= OnInventoryVectors;
            peer.Receiver.OnBlock -= HandleBlock;
            peer.Receiver.OnBlockHeaders -= HandleBlockHeaders;
            peer.Receiver.OnTransaction -= OnTransaction;
            peer.Receiver.OnReceivedAddresses -= OnReceivedAddresses;
            peer.OnGetBlocks -= OnGetBlocks;
            peer.OnGetHeaders -= OnGetHeaders;
            peer.OnGetData -= OnGetData;
            peer.OnPing -= OnPing;
        }

        private void OnMessage(Message message)
        {
            this.messageRateMeasure.Tick();
        }

        private void OnInventoryVectors(ImmutableArray<InventoryVector> invVectors)
        {
            var connectedPeersLocal = this.ConnectedPeers.SafeToList();
            if (connectedPeersLocal.Count == 0)
                return;

            if (this.Type == RulesEnum.ComparisonToolTestNet)
            {
                var responseInvVectors = ImmutableArray.CreateBuilder<InventoryVector>();

                foreach (var invVector in invVectors)
                {
                    if (invVector.Type == InventoryVector.TYPE_MESSAGE_BLOCK
                        && !this.coreStorage.ContainsBlockTxes(invVector.Hash))
                    {
                        responseInvVectors.Add(invVector);
                    }
                }

                connectedPeersLocal.Single().Sender.SendGetData(responseInvVectors.ToImmutable()).Forget();
            }
        }

        private void HandleBlock(Peer peer, Block block)
        {
            var handler = this.OnBlock;
            if (handler != null)
                handler(peer, block);
        }

        private void HandleBlockHeaders(Peer peer, IImmutableList<BlockHeader> blockHeaders)
        {
            var handler = this.OnBlockHeaders;
            if (handler != null)
                handler(peer, blockHeaders);
        }

        private void OnTransaction(Transaction transaction)
        {
            if (logger.IsTraceEnabled)
                logger.Trace("Received transaction {0}".Format2(transaction.Hash));
        }

        private void OnReceivedAddresses(ImmutableArray<NetworkAddressWithTime> addresses)
        {
            var ipEndpoints = new List<IPEndPoint>(addresses.Length);
            foreach (var address in addresses)
            {
                var ipEndpoint = address.NetworkAddress.ToIPEndPoint();
                ipEndpoints.Add(ipEndpoint);
            }

            foreach (var address in addresses)
            {
                this.peerWorker.AddCandidatePeer(address.ToCandidatePeer());

                // store the received address
                // insert if not present, or update if the address time is newer
                //NetworkAddressWithTime knownAddress;
                //if (!this.networkPeerCache.TryGetValue(address.NetworkAddress.GetKey(), out knownAddress)
                //    || knownAddress.Time < address.Time)
                //{
                //    this.networkPeerCache[address.NetworkAddress.GetKey()] = address;
                //}
            }
        }

        private void OnGetBlocks(Peer peer, GetBlocksPayload payload)
        {
            var targetChainLocal = this.coreDaemon.TargetChain;
            if (targetChainLocal == null)
                return;

            ChainedHeader matchingChainedHeader = null;
            foreach (var blockHash in payload.BlockLocatorHashes)
            {
                ChainedHeader chainedHeader;
                if (this.coreStorage.TryGetChainedHeader(blockHash, out chainedHeader))
                {
                    if (chainedHeader.Height < targetChainLocal.Blocks.Count
                        && chainedHeader.Hash == targetChainLocal.Blocks[chainedHeader.Height].Hash)
                    {
                        matchingChainedHeader = chainedHeader;
                        break;
                    }
                }
            }

            if (matchingChainedHeader == null)
            {
                matchingChainedHeader = this.rules.GenesisChainedHeader;
            }

            var limit = 500;
            var invVectors = ImmutableArray.CreateBuilder<InventoryVector>(limit);
            for (var i = matchingChainedHeader.Height; i < targetChainLocal.Blocks.Count && invVectors.Count < limit; i++)
            {
                var chainedHeader = targetChainLocal.Blocks[i];
                invVectors.Add(new InventoryVector(InventoryVector.TYPE_MESSAGE_BLOCK, chainedHeader.Hash));

                if (chainedHeader.Hash == payload.HashStop)
                    break;
            }

            peer.Sender.SendInventory(invVectors.ToImmutable()).Forget();
        }

        private void OnGetHeaders(Peer peer, GetBlocksPayload payload)
        {
            if (this.Type == RulesEnum.ComparisonToolTestNet)
            {
                this.coreDaemon.WaitForUpdate();
            }

            var targetChainLocal = this.coreDaemon.TargetChain;
            if (targetChainLocal == null)
                return;

            ChainedHeader matchingChainedHeader = null;
            foreach (var blockHash in payload.BlockLocatorHashes)
            {
                ChainedHeader chainedHeader;
                if (this.coreStorage.TryGetChainedHeader(blockHash, out chainedHeader))
                {
                    if (chainedHeader.Height < targetChainLocal.Blocks.Count
                        && chainedHeader.Hash == targetChainLocal.Blocks[chainedHeader.Height].Hash)
                    {
                        matchingChainedHeader = chainedHeader;
                        break;
                    }
                }
            }

            if (matchingChainedHeader == null)
            {
                matchingChainedHeader = this.rules.GenesisChainedHeader;
            }

            var limit = 500;
            var blockHeaders = ImmutableArray.CreateBuilder<BlockHeader>(limit);
            for (var i = matchingChainedHeader.Height; i < targetChainLocal.Blocks.Count && blockHeaders.Count < limit; i++)
            {
                var chainedHeader = targetChainLocal.Blocks[i];

                blockHeaders.Add(chainedHeader.BlockHeader);

                if (chainedHeader.Hash == payload.HashStop)
                    break;
            }

            peer.Sender.SendHeaders(blockHeaders.ToImmutable()).Forget();
        }

        private void OnGetData(Peer peer, InventoryPayload payload)
        {
            foreach (var invVector in payload.InventoryVectors)
            {
                switch (invVector.Type)
                {
                    case InventoryVector.TYPE_MESSAGE_BLOCK:
                        //Block block;
                        //if (this.blockCache.TryGetValue(invVector.Hash, out block))
                        //{
                        //    peer.Sender.SendBlock(block).Forget();
                        //}
                        break;

                    case InventoryVector.TYPE_MESSAGE_TRANSACTION:
                        //TODO
                        break;
                }
            }
        }

        private void OnPing(Peer peer, ImmutableArray<byte> payload)
        {
            peer.Sender.SendMessageAsync(Messaging.ConstructMessage("pong", payload.ToArray())).Wait();
        }

        private Task StatsWorker(WorkerMethod instance)
        {
            logger.Info(
                "UNCONNECTED: {0,3}, PENDING: {1,3}, CONNECTED: {2,3}, BAD: {3,3}, INCOMING: {4,3}, MESSAGES/SEC: {5,6:#,##0}".Format2(
                /*0*/ this.peerWorker.UnconnectedPeersCount,
                /*1*/ this.peerWorker.PendingPeers.Count,
                /*2*/ this.peerWorker.ConnectedPeers.Count,
                /*3*/ this.peerWorker.BadPeers.Count,
                /*4*/ this.peerWorker.IncomingCount,
                /*5*/ this.messageRateMeasure.GetAverage(TimeSpan.FromSeconds(1))));

            return Task.FromResult(false);
        }
    }

    internal sealed class CandidatePeer : IComparable<CandidatePeer>
    {
        private readonly IPEndPoint ipEndPoint;
        private readonly DateTime time;
        private readonly bool isSeed;
        private readonly string ipEndPointString;

        public CandidatePeer(IPEndPoint ipEndPoint, DateTime time, bool isSeed)
        {
            this.ipEndPoint = ipEndPoint;
            this.time = time;
            this.isSeed = isSeed;
            this.ipEndPointString = ipEndPoint.ToString();
        }

        public IPEndPoint IPEndPoint { get { return this.ipEndPoint; } }

        public DateTime Time { get { return this.time; } }

        public bool IsSeed { get { return this.isSeed; } }

        public override bool Equals(object obj)
        {
            if (!(obj is CandidatePeer))
                return false;

            var other = (CandidatePeer)obj;
            return other.IPEndPoint.Equals(this.IPEndPoint);
        }

        public override int GetHashCode()
        {
            return this.IPEndPoint.GetHashCode();
        }

        // candidate peers are ordered with seeds last, and then by time
        public int CompareTo(CandidatePeer other)
        {
            if (other.isSeed && !this.isSeed)
                return -1;
            else if (this.isSeed && !other.isSeed)
                return +1;
            else if (other.time < this.time)
                return -1;
            else if (other.time > this.time)
                return +1;
            else
                return this.ipEndPointString.CompareTo(other.ipEndPointString);
        }
    }

    namespace ExtensionMethods
    {
        internal static class LocalClientExtensionMethods
        {
            public static NetworkAddressKey GetKey(this NetworkAddress knownAddress)
            {
                return new NetworkAddressKey(knownAddress.IPv6Address, knownAddress.Port);
            }

            public static NetworkAddressKey ToNetworkAddressKey(this IPEndPoint ipEndPoint)
            {
                return new NetworkAddressKey
                (
                    IPv6Address: Messaging.IPAddressToBytes(ipEndPoint.Address).ToImmutableArray(),
                    Port: (UInt16)ipEndPoint.Port
                );
            }

            public static NetworkAddress ToNetworkAddress(this IPEndPoint ipEndPoint, UInt64 services)
            {
                return new NetworkAddress
                (
                    Services: services,
                    IPv6Address: Messaging.IPAddressToBytes(ipEndPoint.Address).ToImmutableArray(),
                    Port: (UInt16)ipEndPoint.Port
                );
            }

            public static CandidatePeer ToCandidatePeer(this NetworkAddressWithTime address)
            {
                return new CandidatePeer(address.NetworkAddress.ToIPEndPoint(), address.Time.UnixTimeToDateTime(), isSeed: false);
            }
        }
    }
}
