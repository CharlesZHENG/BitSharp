﻿using BitSharp.Blockchain;
using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Daemon;
using BitSharp.Network;
using BitSharp.Network.ExtensionMethods;
using BitSharp.Node.ExtensionMethods;
using BitSharp.Script;
using BitSharp.Storage;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using BitSharp.Data;
using System.IO;
using System.Globalization;

namespace BitSharp.Node
{
    public enum LocalClientType
    {
        MainNet,
        TestNet3,
        ComparisonToolTestNet
    }

    public class LocalClient : IDisposable
    {
        private static readonly int SERVER_BACKLOG = 10;
        private static readonly int CONNECTED_MAX = 25;
        private static readonly int PENDING_MAX = 2 * CONNECTED_MAX;
        private static readonly int HANDSHAKE_TIMEOUT_MS = 15000;

        private static readonly int MAX_BLOCK_REQUESTS = 20 * CONNECTED_MAX;
        private static readonly int MAX_TRANSACTION_REQUESTS = 20 * CONNECTED_MAX;
        private static readonly int MAX_BLOCKCHAIN_LOOKAHEAD = 100.MILLION();
        // don't limit the first 100,000 block downloading as they are so small it will slow down their processing
        private static readonly int MAX_BLOCKCHAIN_LOOKAHEAD_START_HEIGHT = 100.THOUSAND();
        private static readonly int REQUEST_LIFETIME_SECONDS = 5;

        private static readonly Random random = new Random();

        private readonly CancellationTokenSource shutdownToken;

        private readonly LocalClientType _type;

        private readonly WorkerMethod connectWorker;
        private readonly WorkerMethod requestBlocksWorker;
        private readonly WorkerMethod requestHeadersWorker;
        private readonly WorkerMethod requestTransactionsWorker;
        private readonly WorkerMethod statsWorker;

        private readonly BlockchainDaemon blockchainDaemon;

        private IBoundedStorage<NetworkAddressKey, NetworkAddressWithTime> knownAddressStorage;
        private readonly BoundedCache<NetworkAddressKey, NetworkAddressWithTime> knownAddressCache;

        private Stopwatch messageStopwatch = new Stopwatch();
        private int messageCount;

        private int incomingCount;
        private ConcurrentSet<IPEndPoint> unconnectedPeers = new ConcurrentSet<IPEndPoint>();
        private ConcurrentSet<IPEndPoint> badPeers = new ConcurrentSet<IPEndPoint>();
        private ConcurrentDictionary<IPEndPoint, RemoteNode> pendingPeers = new ConcurrentDictionary<IPEndPoint, RemoteNode>();
        private ConcurrentDictionary<IPEndPoint, RemoteNode> connectedPeers = new ConcurrentDictionary<IPEndPoint, RemoteNode>();

        private int requestBlockQueueIndex = 0;
        private List<ChainedBlock> requestBlockQueue;
        private readonly ConcurrentDictionary<UInt256, DateTime> requestedBlocks = new ConcurrentDictionary<UInt256, DateTime>();
        private readonly ConcurrentDictionary<UInt256, DateTime> requestedTransactions = new ConcurrentDictionary<UInt256, DateTime>();

        private Socket listenSocket;

        public LocalClient(LocalClientType type, BlockchainDaemon blockchainDaemon, IBoundedStorage<NetworkAddressKey, NetworkAddressWithTime> knownAddressStorage)
        {
            this._type = type;

            this.blockchainDaemon = blockchainDaemon;
            this.shutdownToken = new CancellationTokenSource();

            this.knownAddressStorage = knownAddressStorage;
            this.knownAddressCache = new BoundedCache<NetworkAddressKey, NetworkAddressWithTime>("KnownAddressCache", knownAddressStorage);

            this.connectWorker = new WorkerMethod("LocalClient.ConnectWorker", ConnectWorker, true, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
            this.requestBlocksWorker = new WorkerMethod("LocalClient.RequestBlocksWorker", RequestBlocksWorker, true, TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(5000));
            this.requestHeadersWorker = new WorkerMethod("LocalClient.RequestHeadersWorker", RequestHeadersWorker, true, TimeSpan.FromMilliseconds(200), TimeSpan.FromMilliseconds(5000));
            this.requestTransactionsWorker = new WorkerMethod("LocalClient.RequestTransactionsWorker", RequestTransactionsWorker, true, TimeSpan.FromMilliseconds(200), TimeSpan.FromMilliseconds(5000));
            this.statsWorker = new WorkerMethod("LocalClient.StatsWorker", StatsWorker, true, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30));

            this.blockchainDaemon.OnTargetChainChanged += OnTargetChainChanged;

            switch (this.Type)
            {
                case LocalClientType.MainNet:
                    Messaging.Port = 8333;
                    Messaging.Magic = Messaging.MAGIC_MAIN;
                    break;

                case LocalClientType.TestNet3:
                    Messaging.Port = 18333;
                    Messaging.Magic = Messaging.MAGIC_TESTNET3;
                    break;

                case LocalClientType.ComparisonToolTestNet:
                    Messaging.Port = 18444;
                    Messaging.Magic = Messaging.MAGIC_COMPARISON_TOOL;
                    break;
            }
        }

        public LocalClientType Type { get { return this._type; } }

        public void Start()
        {
            Startup();

            this.connectWorker.Start();

            this.requestBlocksWorker.Start();
            this.requestHeadersWorker.Start();
            this.requestTransactionsWorker.Start();

            this.messageStopwatch.Start();
            this.messageCount = 0;
            this.statsWorker.Start();
        }

        public void Dispose()
        {
            this.blockchainDaemon.OnTargetChainChanged -= OnTargetChainChanged;

            this.shutdownToken.Cancel();

            Shutdown();

            new IDisposable[]
            {
                this.requestBlocksWorker,
                this.requestHeadersWorker,
                this.requestTransactionsWorker,
                this.connectWorker,
                this.statsWorker,
                this.knownAddressStorage,
                this.shutdownToken
            }.DisposeList();
        }

        private void OnTargetChainChanged(object sender, EventArgs e)
        {
            this.requestHeadersWorker.NotifyWork();
        }

        private void ConnectWorker()
        {
            if (this.Type == LocalClientType.ComparisonToolTestNet)
                return;

            // get peer counts
            var connectedCount = this.connectedPeers.Count;
            var pendingCount = this.pendingPeers.Count;
            var unconnectedCount = this.unconnectedPeers.Count;
            var maxConnections = Math.Max(CONNECTED_MAX + 20, PENDING_MAX);

            // if there aren't enough peers connected and there is a pending connection slot available, make another connection
            if (connectedCount < CONNECTED_MAX
                 && pendingCount < PENDING_MAX
                 && (connectedCount + pendingCount) < maxConnections
                 && unconnectedCount > 0)
            {
                // get number of connections to attempt
                var connectCount = Math.Min(unconnectedCount, maxConnections - (connectedCount + pendingCount));

                var connectTasks = new List<Task>();
                for (var i = 0; i < connectCount; i++)
                {
                    // cooperative loop
                    this.shutdownToken.Token.ThrowIfCancellationRequested();

                    // get a random peer to connect to
                    var remoteEndpoint = this.unconnectedPeers.FirstOrDefault();
                    if (remoteEndpoint != null)
                    {
                        this.unconnectedPeers.Remove(remoteEndpoint);
                        connectTasks.Add(ConnectToPeer(remoteEndpoint));
                    }
                }

                // wait for pending connection attempts to complete
                //Task.WaitAll(connectTasks.ToArray(), this.shutdownToken.Token);
            }

            // check if there are too many peers connected
            var overConnected = this.connectedPeers.Count - CONNECTED_MAX;
            if (overConnected > 0)
            {
                foreach (var remoteEndpoint in this.connectedPeers.Keys.Take(overConnected))
                {
                    // cooperative loop
                    this.shutdownToken.Token.ThrowIfCancellationRequested();

                    //Debug.WriteLine(string.Format("Too many peers connected ({0}), disconnecting {1}", overConnected, remoteEndpoint));
                    DisconnectPeer(remoteEndpoint, null);
                }
            }
        }

        private void RequestBlocksWorker()
        {
            new MethodTimer(false).Time("RequestBlocksWorker", () =>
            {
                var connectedPeersLocal = this.connectedPeers.Values.SafeToList();
                if (connectedPeersLocal.Count == 0)
                    return;

                var now = DateTime.UtcNow;

                // remove old requests
                this.requestedBlocks.RemoveRange(
                    this.requestedBlocks
                    .Where(x => (now - x.Value) > TimeSpan.FromSeconds(REQUEST_LIFETIME_SECONDS))
                    .Select(x => x.Key));

                if (this.requestedBlocks.Count > MAX_BLOCK_REQUESTS)
                    return;

                var requestTasks = new List<Task>();

                //BlockHeader targetBlockHeader;
                //if (!this.blockchainDaemon.CacheContext.BlockHeaderCache.TryGetValue(targetBlockHash, out targetBlockHeader)
                //    || DateTime.UtcNow - targetBlockHeader.Time.UnixTimeToDateTime() > TimeSpan.FromDays(1))
                //{
                //    Debug.WriteLine(targetBlockHeader.Time.UnixTimeToDateTime());
                //    return;
                //}

                // send out requests for any missing blocks
                foreach (var block in this.blockchainDaemon.CacheContext.BlockView.MissingData)
                {
                    //if (requestTasks.Count > requestAmount)
                    if (this.requestedBlocks.Count > MAX_BLOCK_REQUESTS)
                        break;

                    // cooperative loop
                    this.shutdownToken.Token.ThrowIfCancellationRequested();

                    // always allow missing blocks to be requested
                    DateTime ignore;
                    this.requestedBlocks.TryRemove(block, out ignore);

                    var task = RequestBlock(connectedPeersLocal.RandomOrDefault(), block);
                    if (task != null)
                        requestTasks.Add(task);
                }

                if (this.requestBlockQueue == null || this.requestBlockQueueIndex >= this.requestBlockQueue.Count)
                {
                    var chainStateLocal = this.blockchainDaemon.ChainState;
                    var targetChainLocal = this.blockchainDaemon.TargetChain;

                    if (targetChainLocal != null)
                    {
                        this.requestBlockQueue = chainStateLocal.Chain.NavigateTowards(targetChainLocal)
                            .Select(x => x.Item2)
                            .Where(x => !this.blockchainDaemon.CacheContext.BlockView.ContainsKey(x.BlockHash))
                            .Take(MAX_BLOCK_REQUESTS * 10)
                            .ToList();
                        this.requestBlockQueueIndex = 0;
                    }
                }

                if (this.requestBlockQueue != null)
                {
                    for (; this.requestBlockQueueIndex < this.requestBlockQueue.Count; this.requestBlockQueueIndex++)
                    {
                        // cooperative loop
                        this.shutdownToken.Token.ThrowIfCancellationRequested();

                        //if (requestTasks.Count > requestAmount)
                        if (this.requestedBlocks.Count > MAX_BLOCK_REQUESTS)
                            break;

                        var requestBlock = this.requestBlockQueue[this.requestBlockQueueIndex];

                        // limit how far ahead the target blockchain will be downloaded
                        if (requestBlock.Height >= MAX_BLOCKCHAIN_LOOKAHEAD_START_HEIGHT
                            && requestBlock.Height - this.blockchainDaemon.CurrentBuilderHeight > MAX_BLOCKCHAIN_LOOKAHEAD)
                            break;

                        if (!this.blockchainDaemon.CacheContext.BlockView.ContainsKey(requestBlock.BlockHash))
                        {
                            var task = RequestBlock(connectedPeersLocal.RandomOrDefault(), requestBlock.BlockHash);
                            if (task != null)
                                requestTasks.Add(task);
                        }
                    }
                }
            });
        }

        private void RequestHeadersWorker()
        {
            var connectedPeersLocal = this.connectedPeers.Values.SafeToList();
            if (connectedPeersLocal.Count == 0)
                return;

            // send out request for unknown block headers
            if (this.Type != LocalClientType.ComparisonToolTestNet)
                SendGetHeaders(connectedPeersLocal.RandomOrDefault()).Forget();
        }

        private void RequestTransactionsWorker()
        {
            var connectedPeersLocal = this.connectedPeers.Values.SafeToList();
            if (connectedPeersLocal.Count == 0)
                return;

            var now = DateTime.UtcNow;

            // remove old requests
            this.requestedTransactions.RemoveRange(
                this.requestedTransactions
                .Where(x => (now - x.Value) > TimeSpan.FromSeconds(REQUEST_LIFETIME_SECONDS))
                .Select(x => x.Key));

            if (this.requestedTransactions.Count > MAX_TRANSACTION_REQUESTS)
                return;

            var requestTasks = new List<Task>();

            // send out requests for any missing transactions
            foreach (var transaction in this.blockchainDaemon.CacheContext.TransactionCache.MissingData)
            {
                //if (requestTasks.Count > requestAmount)
                if (this.requestedTransactions.Count > MAX_TRANSACTION_REQUESTS)
                    break;

                // cooperative loop
                this.shutdownToken.Token.ThrowIfCancellationRequested();

                var task = RequestTransaction(connectedPeersLocal.RandomOrDefault(), transaction);
                if (task != null)
                    requestTasks.Add(task);
            }
        }

        private void StatsWorker()
        {
            Debug.WriteLine(string.Format("UNCONNECTED: {0,3}, PENDING: {1,3}, CONNECTED: {2,3}, BAD: {3,3}, INCOMING: {4,3}, MESSAGES/SEC: {5,6}", this.unconnectedPeers.Count, this.pendingPeers.Count, this.connectedPeers.Count, this.badPeers.Count, this.incomingCount, ((float)this.messageCount / ((float)this.messageStopwatch.ElapsedMilliseconds / 1000)).ToString("0")));

            this.messageStopwatch.Restart();
            this.messageCount = 0;
        }

        private void Startup()
        {
            Debug.WriteLine("LocalClients starting up");
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            // start listening for incoming peers
            StartListening();

            // add seed peers
            AddSeedPeers();

            // add known peers
            AddKnownPeers();

            stopwatch.Stop();
            Debug.WriteLine("LocalClients finished starting up: {0} ms".Format2(stopwatch.ElapsedMilliseconds));
        }

        private void Shutdown()
        {
            Debug.WriteLine("LocalClient shutting down");

            try
            {
                foreach (var remoteNode in this.connectedPeers.Values)
                {
                    try
                    {
                        remoteNode.Disconnect();
                    }
                    catch (Exception) { } // swallow any exceptions at the peer disconnect level to try and process everyone
                }
            }
            catch (Exception) { } // swallow any looping exceptions

            Debug.WriteLine("LocalClient shutdown finished");
        }

        private async void StartListening()
        {
            try
            {
                switch (this.Type)
                {
                    case LocalClientType.MainNet:
                    case LocalClientType.TestNet3:
                        var externalIPAddress = Messaging.GetExternalIPAddress();
                        var localhost = Dns.GetHostEntry(Dns.GetHostName());

                        this.listenSocket = new Socket(externalIPAddress.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                        this.listenSocket.Bind(new IPEndPoint(localhost.AddressList.Where(x => x.AddressFamily == externalIPAddress.AddressFamily).First(), Messaging.Port));
                        break;

                    case LocalClientType.ComparisonToolTestNet:
                        this.listenSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                        this.listenSocket.Bind(new IPEndPoint(IPAddress.Parse("127.0.0.1"), Messaging.Port));
                        break;
                }
                this.listenSocket.Listen(SERVER_BACKLOG);
            }
            catch (Exception e)
            {
                Debug.WriteLine(e.Message);
                if (this.listenSocket != null)
                    this.listenSocket.Dispose();
                return;
            }

            try
            {
                while (true)
                {
                    // cooperative loop
                    this.shutdownToken.Token.ThrowIfCancellationRequested();

                    try
                    {
                        var newSocket = await Task.Factory.FromAsync<Socket>(this.listenSocket.BeginAccept(null, null), this.listenSocket.EndAccept);

                        Task.Run(async () =>
                        {
                            var remoteNode = new RemoteNode(newSocket);
                            try
                            {
                                if (await ConnectAndHandshake(remoteNode, isIncoming: true))
                                {
                                    Interlocked.Increment(ref this.incomingCount);
                                }
                                else
                                {
                                    DisconnectPeer(remoteNode.RemoteEndPoint, null);
                                }
                            }
                            catch (Exception e)
                            {
                                if (remoteNode.RemoteEndPoint != null)
                                    DisconnectPeer(remoteNode.RemoteEndPoint, e);
                            }
                        }).Forget();
                    }
                    catch (Exception e)
                    {
                        Debug.WriteLine(e.Message);
                    }
                }
            }
            catch (OperationCanceledException) { }
            finally
            {
                this.listenSocket.Dispose();
            }
        }

        private async Task PeerStartup(RemoteNode remoteNode)
        {
            await remoteNode.Sender.RequestKnownAddressesAsync();
        }

        private async Task SendGetHeaders(RemoteNode remoteNode)
        {
            var targetChainLocal = this.blockchainDaemon.TargetChain;
            if (targetChainLocal != null)
            {
                var blockLocatorHashes = CalculateBlockLocatorHashes(targetChainLocal.BlockList);

                await remoteNode.Sender.SendGetHeaders(blockLocatorHashes, hashStop: 0);
            }
        }

        private async Task SendGetBlocks(RemoteNode remoteNode)
        {
            var targetChainLocal = this.blockchainDaemon.TargetChain;
            if (targetChainLocal != null)
            {
                var blockLocatorHashes = CalculateBlockLocatorHashes(targetChainLocal.BlockList);

                await remoteNode.Sender.SendGetBlocks(blockLocatorHashes, hashStop: 0);
            }
        }

        private void AddSeedPeers()
        {
            Action<string> addSeed =
                hostNameOrAddress =>
                {
                    try
                    {
                        var ipAddress = Dns.GetHostEntry(hostNameOrAddress).AddressList.First();
                        this.unconnectedPeers.TryAdd(new IPEndPoint(ipAddress, Messaging.Port));
                    }
                    catch (SocketException e)
                    {
                        Debug.WriteLine("Failed to add seed peer {0}: {1}".Format2(hostNameOrAddress, e.Message));
                    }
                };

            switch (this.Type)
            {
                case LocalClientType.MainNet:
                    addSeed("archivum.info");
                    addSeed("62.75.216.13");
                    addSeed("69.64.34.118");
                    addSeed("79.160.221.140");
                    addSeed("netzbasis.de");
                    addSeed("btc.turboadmin.com");
                    addSeed("fallback.bitcoin.zhoutong.com");
                    addSeed("bauhaus.csail.mit.edu");
                    addSeed("jun.dashjr.org");
                    addSeed("cheaperinbitcoins.com");
                    addSeed("django.webflows.fr");
                    addSeed("204.9.55.71");
                    addSeed("btcnode.novit.ro");
                    //No such host is known: addSeed("porgressbar.sk");
                    addSeed("faucet.bitcoin.st");
                    addSeed("bitcoin.securepayment.cc");
                    addSeed("x.jine.se");
                    //addSeed("www.dcscdn.com");
                    //addSeed("ns2.dcscdn.com");
                    //No such host is known: addSeed("coin.soul-dev.com");
                    addSeed("messier.bzfx.net");
                    break;

                case LocalClientType.TestNet3:
                    addSeed("testnet-seed.bitcoin.petertodd.org");
                    addSeed("testnet-seed.bluematt.me");
                    break;
            }
        }

        private void AddKnownPeers()
        {
            var count = 0;
            foreach (var knownAddress in this.knownAddressCache.Values)
            {
                this.unconnectedPeers.TryAdd(knownAddress.NetworkAddress.ToIPEndPoint());
                count++;
            }

            Debug.WriteLine("LocalClients loaded {0} known peers from database".Format2(count));
        }

        private async Task<RemoteNode> ConnectToPeer(IPEndPoint remoteEndPoint)
        {
            try
            {
                var remoteNode = new RemoteNode(remoteEndPoint);

                this.unconnectedPeers.TryRemove(remoteEndPoint);
                this.pendingPeers.TryAdd(remoteNode.RemoteEndPoint, remoteNode);

                var success = await ConnectAndHandshake(remoteNode, isIncoming: false);
                if (success)
                {
                    await PeerStartup(remoteNode);

                    return remoteNode;
                }
                else
                {
                    DisconnectPeer(remoteEndPoint, null);
                    return null;
                }
            }
            catch (Exception e)
            {
                //Debug.WriteLine(string.Format("Could not connect to {0}: {1}", remoteEndpoint, e.Message));
                DisconnectPeer(remoteEndPoint, e);
                return null;
            }
        }

        private void WireNode(RemoteNode remoteNode)
        {
            remoteNode.Receiver.OnMessage += OnMessage;
            remoteNode.Receiver.OnInventoryVectors += OnInventoryVectors;
            remoteNode.Receiver.OnBlock += OnBlock;
            remoteNode.Receiver.OnBlockHeader += OnBlockHeader;
            remoteNode.Receiver.OnTransaction += OnTransaction;
            remoteNode.Receiver.OnReceivedAddresses += OnReceivedAddresses;
            remoteNode.OnGetBlocks += OnGetBlocks;
            remoteNode.OnGetHeaders += OnGetHeaders;
            remoteNode.OnGetData += OnGetData;
            remoteNode.OnPing += OnPing;
            remoteNode.OnDisconnect += OnDisconnect;
        }

        private void UnwireNode(RemoteNode remoteNode)
        {
            remoteNode.Receiver.OnMessage -= OnMessage;
            remoteNode.Receiver.OnInventoryVectors -= OnInventoryVectors;
            remoteNode.Receiver.OnBlock -= OnBlock;
            remoteNode.Receiver.OnBlockHeader -= OnBlockHeader;
            remoteNode.Receiver.OnTransaction -= OnTransaction;
            remoteNode.Receiver.OnReceivedAddresses -= OnReceivedAddresses;
            remoteNode.OnGetBlocks -= OnGetBlocks;
            remoteNode.OnGetHeaders -= OnGetHeaders;
            remoteNode.OnGetData -= OnGetData;
            remoteNode.OnPing -= OnPing;
            remoteNode.OnDisconnect -= OnDisconnect;
        }

        private void OnMessage(Message message)
        {
            Interlocked.Increment(ref this.messageCount);
        }

        private void OnInventoryVectors(ImmutableArray<InventoryVector> invVectors)
        {
            var connectedPeersLocal = this.connectedPeers.Values.SafeToList();
            if (connectedPeersLocal.Count == 0)
                return;

            if (this.Type == LocalClientType.ComparisonToolTestNet)
            {
                foreach (var invVector in invVectors)
                {
                    if (invVector.Type == InventoryVector.TYPE_MESSAGE_BLOCK
                        && !this.blockchainDaemon.CacheContext.BlockView.ContainsKey(invVector.Hash))
                    {
                        this.RequestBlock(connectedPeersLocal.RandomOrDefault(), invVector.Hash);
                    }
                }
            }
        }

        private Task RequestBlock(RemoteNode remoteNode, UInt256 blockHash)
        {
            if (this.blockchainDaemon.CacheContext.BlockView.ContainsKey(blockHash))
                return null;

            var now = DateTime.UtcNow;

            // check if block has already been requested
            if (this.requestedBlocks.TryAdd(blockHash, now))
            {
                var invVectors = ImmutableArray.Create<InventoryVector>(new InventoryVector(InventoryVector.TYPE_MESSAGE_BLOCK, blockHash));
                return remoteNode.Sender.SendGetData(invVectors);
            }

            return null;
        }

        private Task RequestTransaction(RemoteNode remoteNode, UInt256 txHash)
        {
            //TODO
            //if (this.blockchainDaemon.CacheContext.TransactionCache.ContainsKey(txHash))
            //    return null;

            var now = DateTime.UtcNow;
            var newRequestTime = now;

            // check if transaction has already been requested
            if (this.requestedTransactions.TryAdd(txHash, now))
            {
                var invVectors = ImmutableArray.Create<InventoryVector>(new InventoryVector(InventoryVector.TYPE_MESSAGE_TRANSACTION, txHash));
                return remoteNode.Sender.SendGetData(invVectors);
            }

            return null;
        }

        private void OnBlock(Block block)
        {
            //Debug.WriteLine("Received block {0}".Format2(block.Hash));

            DateTime ignore;
            this.requestedBlocks.TryRemove(block.Hash, out ignore);
            this.blockchainDaemon.CacheContext.BlockView.TryAdd(block.Hash, block);

            this.requestBlocksWorker.NotifyWork();
        }

        private void OnBlockHeader(BlockHeader blockHeader)
        {
            //Debug.WriteLine("Received block header {0}".Format2(blockHeader.Hash);
            this.blockchainDaemon.CacheContext.BlockHeaderCache.TryAdd(blockHeader.Hash, blockHeader);
        }

        private void OnTransaction(Transaction transaction)
        {
            //Debug.WriteLine("Received block header {0}".Format2(transaction.Hash);

            DateTime ignore;
            this.requestedTransactions.TryRemove(transaction.Hash, out ignore);
            this.blockchainDaemon.CacheContext.TransactionCache.TryAdd(transaction.Hash, transaction);

            this.requestTransactionsWorker.NotifyWork();
        }

        private void OnReceivedAddresses(ImmutableArray<NetworkAddressWithTime> addresses)
        {
            var ipEndpoints = new List<IPEndPoint>(addresses.Count);
            foreach (var address in addresses)
            {
                var ipEndpoint = address.NetworkAddress.ToIPEndPoint();
                ipEndpoints.Add(ipEndpoint);
            }

            this.unconnectedPeers.UnionWith(ipEndpoints);
            this.unconnectedPeers.ExceptWith(this.badPeers);
            this.unconnectedPeers.ExceptWith(this.connectedPeers.Keys);
            this.unconnectedPeers.ExceptWith(this.pendingPeers.Keys);

            // queue up addresses to be flushed to the database
            foreach (var address in addresses)
            {
                //TODO enable this once i start filtering out garbage
                //this.knownAddressCache.UpdateValue(address.GetKey(), address);
            }
        }

        private void OnGetBlocks(RemoteNode remoteNode, GetBlocksPayload payload)
        {
            var targetChainLocal = this.blockchainDaemon.TargetChain;
            if (targetChainLocal == null)
                return;

            ChainedBlock matchingChainedBlock = null;
            foreach (var blockHash in payload.BlockLocatorHashes)
            {
                ChainedBlock chainedBlock;
                if (this.blockchainDaemon.CacheContext.ChainedBlockCache.TryGetValue(blockHash, out chainedBlock))
                {
                    if (chainedBlock.Height < targetChainLocal.BlockList.Count
                        && chainedBlock.BlockHash == targetChainLocal.BlockList[chainedBlock.Height].BlockHash)
                    {
                        matchingChainedBlock = chainedBlock;
                        break;
                    }
                }
            }

            if (matchingChainedBlock == null)
            {
                matchingChainedBlock = this.blockchainDaemon.Rules.GenesisChainedBlock;
            }

            var count = 0;
            var limit = 500;
            var invVectors = new InventoryVector[limit];
            for (var i = matchingChainedBlock.Height; i < targetChainLocal.BlockList.Count && count <= limit; i++, count++)
            {
                var chainedBlock = targetChainLocal.BlockList[i];
                invVectors[count] = new InventoryVector(InventoryVector.TYPE_MESSAGE_BLOCK, chainedBlock.BlockHash);

                if (chainedBlock.BlockHash == payload.HashStop)
                    break;
            }
            Array.Resize(ref invVectors, count);

            remoteNode.Sender.SendInventory(invVectors.ToImmutableArray()).Forget();
        }

        private void OnGetHeaders(RemoteNode remoteNode, GetBlocksPayload payload)
        {
            if (this.Type == LocalClientType.ComparisonToolTestNet)
            {
                this.blockchainDaemon.ForceWorkAndWait();
            }

            var targetChainLocal = this.blockchainDaemon.TargetChain;
            if (targetChainLocal == null)
                return;

            ChainedBlock matchingChainedBlock = null;
            foreach (var blockHash in payload.BlockLocatorHashes)
            {
                ChainedBlock chainedBlock;
                if (this.blockchainDaemon.CacheContext.ChainedBlockCache.TryGetValue(blockHash, out chainedBlock))
                {
                    if (chainedBlock.Height < targetChainLocal.BlockList.Count
                        && chainedBlock.BlockHash == targetChainLocal.BlockList[chainedBlock.Height].BlockHash)
                    {
                        matchingChainedBlock = chainedBlock;
                        break;
                    }
                }
            }

            if (matchingChainedBlock == null)
            {
                matchingChainedBlock = this.blockchainDaemon.Rules.GenesisChainedBlock;
            }

            var count = 0;
            var limit = 500;
            var blockHeaders = new BlockHeader[limit];
            for (var i = matchingChainedBlock.Height; i < targetChainLocal.BlockList.Count && count <= limit; i++, count++)
            {
                var chainedBlock = targetChainLocal.BlockList[i];

                BlockHeader blockHeader;
                if (this.blockchainDaemon.CacheContext.BlockHeaderCache.TryGetValue(targetChainLocal.BlockList[i].BlockHash, out blockHeader))
                {
                    blockHeaders[count] = blockHeader;
                }
                else
                {
                    break;
                }

                if (chainedBlock.BlockHash == payload.HashStop)
                    break;
            }
            Array.Resize(ref blockHeaders, count);

            remoteNode.Sender.SendHeaders(blockHeaders.ToImmutableArray()).Forget();
        }

        private void OnGetData(RemoteNode remoteNode, InventoryPayload payload)
        {
            foreach (var invVector in payload.InventoryVectors)
            {
                switch (invVector.Type)
                {
                    case InventoryVector.TYPE_MESSAGE_BLOCK:
                        Block block;
                        if (this.blockchainDaemon.CacheContext.BlockView.TryGetValue(invVector.Hash, out block))
                        {
                            //remoteNode.Sender.SendBlock(block).Forget();
                        }
                        break;

                    case InventoryVector.TYPE_MESSAGE_TRANSACTION:
                        Transaction transaction;
                        if (this.blockchainDaemon.CacheContext.TransactionCache.TryGetValue(invVector.Hash, out transaction))
                        {
                            remoteNode.Sender.SendTransaction(transaction).Forget();
                        }
                        break;
                }
            }
        }

        private void OnPing(RemoteNode remoteNode, ImmutableArray<byte> payload)
        {
            remoteNode.Sender.SendMessageAsync(Messaging.ConstructMessage("pong", payload.ToArray())).Wait();
        }

        private void OnDisconnect(RemoteNode remoteNode)
        {
            DisconnectPeer(remoteNode.RemoteEndPoint, null);
        }

        private async Task<bool> ConnectAndHandshake(RemoteNode remoteNode, bool isIncoming)
        {
            // wire node
            WireNode(remoteNode);

            // connect
            await remoteNode.ConnectAsync();

            if (remoteNode.IsConnected)
            {
                //TODO
                RemoteNode ignore;
                this.pendingPeers.TryRemove(remoteNode.RemoteEndPoint, out ignore);
                this.connectedPeers.TryAdd(remoteNode.RemoteEndPoint, remoteNode);

                // setup task to wait for verack
                var verAckTask = remoteNode.Receiver.WaitForMessage(x => x.Command == "verack", HANDSHAKE_TIMEOUT_MS);

                // setup task to wait for version
                var versionTask = remoteNode.Receiver.WaitForMessage(x => x.Command == "version", HANDSHAKE_TIMEOUT_MS);

                // start listening for messages after tasks have been setup
                remoteNode.Receiver.Listen();

                // send our local version
                var nodeId = (((UInt64)random.Next()) << 32) + (UInt64)random.Next(); //TODO should be generated and verified on version message

                var currentHeight = this.blockchainDaemon.ChainState.Height;
                await remoteNode.Sender.SendVersion(Messaging.GetExternalIPEndPoint(), remoteNode.RemoteEndPoint, nodeId, (UInt32)currentHeight);

                // wait for our local version to be acknowledged by the remote peer
                // wait for remote peer to send their version
                await Task.WhenAll(verAckTask, versionTask);

                //TODO shouldn't have to decode again
                var versionMessage = versionTask.Result;
                var versionPayload = NetworkEncoder.DecodeVersionPayload(versionMessage.Payload.ToArray(), versionMessage.Payload.Count);

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

                if (!isIncoming)
                    this.knownAddressCache[remoteAddressWithTime.GetKey()] = remoteAddressWithTime;

                // acknowledge their version
                await remoteNode.Sender.SendVersionAcknowledge();

                return true;
            }
            else
            {
                return false;
            }
        }

        private void DisconnectPeer(IPEndPoint remoteEndpoint, Exception e)
        {
            this.badPeers.Add(remoteEndpoint); //TODO
            this.unconnectedPeers.Remove(remoteEndpoint);

            RemoteNode pendingPeer;
            this.pendingPeers.TryRemove(remoteEndpoint, out pendingPeer);

            RemoteNode connectedPeer;
            this.connectedPeers.TryRemove(remoteEndpoint, out connectedPeer);

            if (this.connectedPeers.Count <= 10 && e != null)
            {
                Debug.WriteLine("Remote peer {0} failed, disconnecting: {1}".Format2(remoteEndpoint, e != null ? e.Message : null));
            }

            if (pendingPeer != null)
            {
                UnwireNode(pendingPeer);
                pendingPeer.Disconnect();
            }

            if (connectedPeer != null)
            {
                UnwireNode(connectedPeer);
                connectedPeer.Disconnect();
            }
        }

        //TODO move into p2p node
        private static ImmutableArray<UInt256> CalculateBlockLocatorHashes(IImmutableList<ChainedBlock> blockHashes)
        {
            var blockLocatorHashes = new List<UInt256>();

            if (blockHashes.Count > 0)
            {
                var step = 1;
                var start = 0;
                for (var i = blockHashes.Count - 1; i > 0; i -= step, start++)
                {
                    if (start >= 10)
                        step *= 2;

                    blockLocatorHashes.Add(blockHashes[i].BlockHash);
                }
                blockLocatorHashes.Add(blockHashes[0].BlockHash);
            }

            return blockLocatorHashes.ToImmutableArray();
        }
    }

    namespace ExtensionMethods
    {
        internal static class LocalClientExtensionMethods
        {
            public static NetworkAddressKey GetKey(this NetworkAddressWithTime knownAddress)
            {
                return new NetworkAddressKey(knownAddress.NetworkAddress.IPv6Address, knownAddress.NetworkAddress.Port);
            }
        }
    }
}
