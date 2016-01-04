using BitSharp.Common;
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
using System.Collections.Concurrent;
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

        private readonly ChainType type;
        private readonly IKernel kernel;
        private readonly IChainParams chainParams;
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

        //TODO properly organize comparison tool code
        private readonly ConcurrentSet<UInt256> requestedComparisonBlocks = new ConcurrentSet<UInt256>();
        private readonly ConcurrentDictionary<UInt256, Block> comparisonUnchainedBlocks = new ConcurrentDictionary<UInt256, Block>();

        private readonly AutoResetEvent comparisonBlockAddedEvent = new AutoResetEvent(false);
        private readonly ManualResetEvent comparisonHeadersSentEvent = new ManualResetEvent(true);

        public LocalClient(ChainType type, IKernel kernel, CoreDaemon coreDaemon, INetworkPeerStorage networkPeerStorage)
        {
            this.shutdownToken = new CancellationTokenSource();

            this.type = type;
            this.kernel = kernel;
            this.coreDaemon = coreDaemon;
            this.chainParams = coreDaemon.ChainParams;
            this.coreStorage = coreDaemon.CoreStorage;
            this.networkPeerStorage = networkPeerStorage;

            this.messageRateMeasure = new RateMeasure();

            this.headersRequestWorker = new HeadersRequestWorker(
                new WorkerConfig(initialNotify: true, minIdleTime: TimeSpan.FromMilliseconds(50), maxIdleTime: TimeSpan.FromSeconds(5)),
                this, this.coreDaemon);

            this.blockRequestWorker = new BlockRequestWorker(
                new WorkerConfig(initialNotify: true, minIdleTime: TimeSpan.FromMilliseconds(50), maxIdleTime: TimeSpan.FromSeconds(30)),
                this, this.coreDaemon);

            this.peerWorker = new PeerWorker(
                new WorkerConfig(initialNotify: true, minIdleTime: TimeSpan.FromSeconds(1), maxIdleTime: TimeSpan.FromSeconds(1)),
                this, this.coreDaemon, this.headersRequestWorker);

            this.listenWorker = new ListenWorker(this, this.peerWorker);

            this.statsWorker = new WorkerMethod("LocalClient.StatsWorker", StatsWorker, true, TimeSpan.FromSeconds(5), TimeSpan.FromMinutes(5));

            this.peerWorker.PeerHandshakeCompleted += HandlePeerHandshakeCompleted;
            this.peerWorker.PeerDisconnected += HandlePeerDisconnected;

            this.blockRequestWorker.OnBlockFlushed += HandleBlockFlushed;

            switch (this.Type)
            {
                case ChainType.MainNet:
                    Messaging.Port = 8333;
                    Messaging.Magic = Messaging.MAGIC_MAIN;
                    break;

                case ChainType.TestNet3:
                    Messaging.Port = 18333;
                    Messaging.Magic = Messaging.MAGIC_TESTNET3;
                    break;

                case ChainType.Regtest:
                    Messaging.Port = 18444;
                    Messaging.Magic = Messaging.MAGIC_COMPARISON_TOOL;
                    break;
            }
        }

        public ChainType Type => this.type;

        internal ConcurrentSet<Peer> ConnectedPeers => this.peerWorker.ConnectedPeers;

        public void Start(bool connectToPeers = true)
        {
            if (this.Type != ChainType.Regtest)
                this.headersRequestWorker.Start();

            this.blockRequestWorker.Start();

            this.statsWorker.Start();

            if (connectToPeers)
            {
                this.peerWorker.Start();
                if (this.Type != ChainType.Regtest)
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

                this.peerWorker.PeerHandshakeCompleted -= HandlePeerHandshakeCompleted;
                this.peerWorker.PeerDisconnected -= HandlePeerDisconnected;

                this.blockRequestWorker.OnBlockFlushed -= HandleBlockFlushed;

                this.messageRateMeasure.Dispose();
                this.statsWorker.Dispose();
                this.headersRequestWorker.Dispose();
                this.blockRequestWorker.Dispose();
                this.peerWorker.Dispose();
                this.listenWorker.Dispose();
                this.shutdownToken.Dispose();

                this.comparisonBlockAddedEvent.Dispose();
                this.comparisonHeadersSentEvent.Dispose();

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
                                isSeed: this.Type == ChainType.MainNet ? true : false
                            ));
                    }
                    catch (SocketException ex)
                    {
                        logger.Warn(ex, $"Failed to add seed peer {hostNameOrAddress}");
                    }
                };

            switch (this.Type)
            {
                case ChainType.MainNet:
                    addSeed("seed.bitcoin.sipa.be");
                    addSeed("dnsseed.bluematt.me");
                    //addSeed("dnsseed.bitcoin.dashjr.org");
                    addSeed("seed.bitcoinstats.com");
                    //addSeed("seed.bitnodes.io");
                    //addSeed("seeds.bitcoin.open-nodes.org");
                    addSeed("bitseed.xf2.org");
                    break;

                case ChainType.TestNet3:
                    addSeed("testnet-seed.alexykot.me");
                    addSeed("testnet-seed.bitcoin.petertodd.org");
                    addSeed("testnet-seed.bluematt.me");
                    break;
            }
        }

        private void AddKnownPeers()
        {
            var count = 0;
            foreach (var knownAddress in this.networkPeerStorage)
            {
                this.peerWorker.AddCandidatePeer(
                    new CandidatePeer
                    (
                        ipEndPoint: knownAddress.Value.NetworkAddress.ToIPEndPoint(),
                        time: knownAddress.Value.Time.UnixTimeToDateTime() + TimeSpan.FromDays(random.NextDouble(-2, +2)),
                        isSeed: false
                    ));
                count++;
            }

            logger.Info($"LocalClients loaded {count} known peers from database");
        }

        private void HandlePeerHandshakeCompleted(Peer peer)
        {
            var remoteAddressWithTime = new NetworkAddressWithTime(DateTime.UtcNow.ToUnixTime(), peer.RemoteEndPoint.ToNetworkAddress(/*TODO*/services: 0));
            this.networkPeerStorage[remoteAddressWithTime.NetworkAddress.GetKey()] = remoteAddressWithTime;

            WirePeerEvents(peer);

            this.statsWorker.NotifyWork();
            this.blockRequestWorker.NotifyWork();
        }

        private void HandlePeerDisconnected(Peer peer)
        {
            UnwirePeerEvents(peer);

            this.statsWorker.NotifyWork();
            this.blockRequestWorker.NotifyWork();
        }

        private void HandleBlockFlushed(Object sender, Block block)
        {
            if (type == ChainType.Regtest)
            {
                // the block won't have been added if it doesn't chain onto another block, hold onto it to try again later
                if (!coreStorage.ContainsBlockTxes(block.Hash))
                {
                    // don't add the block back in if it was deleted due to CVE-2012-2459 handling
                    if (!block.Transactions.AnyDuplicates(x => x.Hash))
                        comparisonUnchainedBlocks.TryAdd(block.Hash, block);
                }
                // now that a block has been added, try any unchained blocks again
                else
                {
                    foreach (var unchainedBlock in comparisonUnchainedBlocks.Values.ToList())
                    {
                        Block ignore;
                        if (coreStorage.TryAddBlock(unchainedBlock))
                        {
                            comparisonUnchainedBlocks.TryRemove(unchainedBlock.Hash, out ignore);

                            //TODO this should be handled better elsewhere
                            if (coreStorage.IsBlockInvalid(block.Header.PreviousBlock))
                                coreStorage.MarkBlockInvalid(block.Hash, coreDaemon.TargetChain);
                        }
                    }
                }

                this.coreDaemon.ForceUpdateAndWait();

                requestedComparisonBlocks.Remove(block.Hash);
                comparisonBlockAddedEvent.Set();
            }
        }

        private void WirePeerEvents(Peer peer)
        {
            peer.Receiver.OnMessage += OnMessage;
            peer.Receiver.OnInventoryVectors += OnInventoryVectors;
            peer.Receiver.OnBlock += HandleBlock;
            peer.Receiver.OnBlockHeaders += HandleBlockHeaders;
            peer.Receiver.OnTransaction += OnTransaction;
            peer.Receiver.OnReceivedAddresses += OnReceivedAddresses;
            peer.Receiver.OnGetBlocks += OnGetBlocks;
            peer.Receiver.OnGetHeaders += OnGetHeaders;
            peer.Receiver.OnGetData += OnGetData;
            peer.Receiver.OnPing += OnPing;
        }

        private void UnwirePeerEvents(Peer peer)
        {
            peer.Receiver.OnMessage -= OnMessage;
            peer.Receiver.OnInventoryVectors -= OnInventoryVectors;
            peer.Receiver.OnBlock -= HandleBlock;
            peer.Receiver.OnBlockHeaders -= HandleBlockHeaders;
            peer.Receiver.OnTransaction -= OnTransaction;
            peer.Receiver.OnReceivedAddresses -= OnReceivedAddresses;
            peer.Receiver.OnGetBlocks -= OnGetBlocks;
            peer.Receiver.OnGetHeaders -= OnGetHeaders;
            peer.Receiver.OnGetData -= OnGetData;
            peer.Receiver.OnPing -= OnPing;
        }

        private void OnMessage(Peer peer, Message message)
        {
            this.messageRateMeasure.Tick();
        }

        private void OnInventoryVectors(Peer peer, ImmutableArray<InventoryVector> invVectors)
        {
            var connectedPeersLocal = this.ConnectedPeers.SafeToList();
            if (connectedPeersLocal.Count == 0)
                return;

            if (this.Type != ChainType.Regtest)
            {
                var responseInvVectors = ImmutableArray.CreateBuilder<InventoryVector>();

                using (var chainState = coreDaemon.GetChainState())
                using (var unconfirmedTxes = coreDaemon.GetUnconfirmedTxes())
                {
                    foreach (var invVector in invVectors)
                    {
                        // check if this is a transaction we don't have yet
                        if (invVector.Type == InventoryVector.TYPE_MESSAGE_TRANSACTION
                            && !unconfirmedTxes.ContainsTransaction(invVector.Hash)
                            && !chainState.ContainsUnspentTx(invVector.Hash))
                        {
                            //logger.Info($"Requesting transaction {invVector.Hash}");
                            responseInvVectors.Add(invVector);
                        }
                        // check if this is a block we don't have yet
                        else if (invVector.Type == InventoryVector.TYPE_MESSAGE_TRANSACTION
                            && !coreStorage.ContainsChainedHeader(invVector.Hash))
                        {
                            // ask for headers on a new block
                            headersRequestWorker.SendGetHeaders(peer);
                        }
                    }
                }

                // request missing transactions
                if (responseInvVectors.Count > 0)
                    peer.Sender.SendGetData(responseInvVectors.ToImmutable()).Wait();
            }
            else
            {
                // don't process new inv request until previous inv request has finished
                while (this.requestedComparisonBlocks.Count > 0 && comparisonBlockAddedEvent.WaitOne(1000))
                { }
                this.coreDaemon.ForceUpdateAndWait();

                var responseInvVectors = ImmutableArray.CreateBuilder<InventoryVector>();

                foreach (var invVector in invVectors)
                {
                    if (invVector.Type == InventoryVector.TYPE_MESSAGE_BLOCK
                        && !requestedComparisonBlocks.Contains(invVector.Hash)
                        && !comparisonUnchainedBlocks.ContainsKey(invVector.Hash)
                        && !this.coreStorage.ContainsBlockTxes(invVector.Hash))
                    {
                        logger.Info($"processing block inv: {invVector.Hash}");
                        responseInvVectors.Add(invVector);
                        requestedComparisonBlocks.Add(invVector.Hash);
                    }
                    else
                    {
                        logger.Info($"ignoring block inv: {invVector.Hash}, exists: {coreStorage.ContainsBlockTxes(invVector.Hash)}");
                    }
                }

                if (responseInvVectors.Count > 0)
                    connectedPeersLocal.Single().Sender.SendGetData(responseInvVectors.ToImmutable()).Wait();
            }
        }

        private void HandleBlock(Peer peer, Block block)
        {
            this.OnBlock?.Invoke(peer, block);
        }

        private void HandleBlockHeaders(Peer peer, IImmutableList<BlockHeader> blockHeaders)
        {
            this.OnBlockHeaders?.Invoke(peer, blockHeaders);
        }

        private void OnTransaction(Peer peer, Transaction transaction)
        {
            var result = coreDaemon.TryAddUnconfirmedTx(transaction);

            //logger.Info($"Received transaction {transaction.Hash}: {result}");
        }

        private void OnReceivedAddresses(Peer peer, ImmutableArray<NetworkAddressWithTime> addresses)
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
                matchingChainedHeader = this.chainParams.GenesisChainedHeader;
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
            if (this.Type == ChainType.Regtest)
            {
                // don't send headers until all blocks requested from the comparison tool have been downloaded and processed
                while (this.requestedComparisonBlocks.Count > 0 && comparisonBlockAddedEvent.WaitOne(1000))
                { }
                this.coreDaemon.ForceUpdateAndWait();
            }

            var currentChainLocal = this.coreDaemon.CurrentChain;
            if (currentChainLocal == null)
                return;

            ChainedHeader matchingChainedHeader = null;
            foreach (var blockHash in payload.BlockLocatorHashes)
            {
                ChainedHeader chainedHeader;
                if (this.coreStorage.TryGetChainedHeader(blockHash, out chainedHeader))
                {
                    if (chainedHeader.Height < currentChainLocal.Blocks.Count
                        && chainedHeader.Hash == currentChainLocal.Blocks[chainedHeader.Height].Hash)
                    {
                        matchingChainedHeader = chainedHeader;
                        break;
                    }
                }
            }

            if (matchingChainedHeader == null)
            {
                matchingChainedHeader = this.chainParams.GenesisChainedHeader;
            }

            var limit = 500;
            var blockHeaders = ImmutableArray.CreateBuilder<BlockHeader>(limit);
            for (var i = matchingChainedHeader.Height; i < currentChainLocal.Blocks.Count && blockHeaders.Count < limit; i++)
            {
                var chainedHeader = currentChainLocal.Blocks[i];

                blockHeaders.Add(chainedHeader.BlockHeader);

                if (chainedHeader.Hash == payload.HashStop)
                    break;
            }

            comparisonHeadersSentEvent.Reset();
            var sendTask = peer.Sender.SendHeaders(blockHeaders.ToImmutable())
                .ContinueWith(_ => comparisonHeadersSentEvent.Set());

            if (type == ChainType.Regtest)
                sendTask.Wait();
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
            if (this.Type == ChainType.Regtest)
            {
                // don't pong back until:
                // - all blocks requested from the comparison tool have been downloaded and processed
                // - current header inventory has been sent to the comparison tool
                while (comparisonHeadersSentEvent.WaitOne(1000)
                    && this.requestedComparisonBlocks.Count > 0 && comparisonBlockAddedEvent.WaitOne(1000))
                { }
            }

            peer.Sender.SendMessageAsync(Messaging.ConstructMessage("pong", payload.ToArray())).Wait();
        }

        private Task StatsWorker(WorkerMethod instance)
        {
            logger.Info(string.Join(", ",
                $"UNCONNECTED: {this.peerWorker.UnconnectedPeersCount,3}",
                $"PENDING: {this.peerWorker.PendingPeers.Count,3}",
                $"CONNECTED: {this.peerWorker.ConnectedPeers.Count,3}",
                $"BAD: {this.peerWorker.BadPeers.Count,3}",
                $"INCOMING: {this.peerWorker.IncomingCount,3}",
                $"MESSAGES/SEC: {this.messageRateMeasure.GetAverage(),6:N0}"
            ));

            return Task.FromResult(false);
        }
    }

    public sealed class CandidatePeer : IComparable<CandidatePeer>
    {
        private readonly string ipEndPointString;

        public CandidatePeer(IPEndPoint ipEndPoint, DateTime time, bool isSeed)
        {
            IPEndPoint = ipEndPoint;
            Time = time;
            IsSeed = isSeed;
            ipEndPointString = ipEndPoint.ToString();
        }

        public IPEndPoint IPEndPoint { get; }

        public DateTime Time { get; }

        public bool IsSeed { get; }

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
            if (other.IsSeed && !this.IsSeed)
                return -1;
            else if (this.IsSeed && !other.IsSeed)
                return +1;
            else if (other.Time < this.Time)
                return -1;
            else if (other.Time > this.Time)
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
