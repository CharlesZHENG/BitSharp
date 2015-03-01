using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core;
using BitSharp.Core.Domain;
using BitSharp.Core.Storage;
using BitSharp.Node.Network;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading.Tasks;

namespace BitSharp.Node.Workers
{
    internal class HeadersRequestWorker : Worker
    {
        private static readonly TimeSpan STALE_REQUEST_TIME = TimeSpan.FromSeconds(60);

        private readonly Logger logger;
        private readonly LocalClient localClient;
        private readonly CoreDaemon coreDaemon;
        private readonly CoreStorage coreStorage;

        private readonly ConcurrentDictionary<Peer, DateTime> headersRequestsByPeer;

        private readonly WorkerMethod flushWorker;
        private readonly ConcurrentQueue<FlushHeaders> flushQueue;

        public HeadersRequestWorker(WorkerConfig workerConfig, Logger logger, LocalClient localClient, CoreDaemon coreDaemon)
            : base("HeadersRequestWorker", workerConfig.initialNotify, workerConfig.minIdleTime, workerConfig.maxIdleTime, logger)
        {
            this.logger = logger;
            this.localClient = localClient;
            this.coreDaemon = coreDaemon;
            this.coreStorage = coreDaemon.CoreStorage;

            this.headersRequestsByPeer = new ConcurrentDictionary<Peer, DateTime>();

            this.localClient.OnBlockHeaders += HandleBlockHeaders;
            this.coreDaemon.OnTargetChainChanged += HandleTargetChainChanged;

            this.flushWorker = new WorkerMethod("HeadersRequestWorker.FlushWorker", FlushWorkerMethod, initialNotify: true, minIdleTime: TimeSpan.Zero, maxIdleTime: TimeSpan.MaxValue, logger: this.logger);
            this.flushQueue = new ConcurrentQueue<FlushHeaders>();
        }

        protected override void SubDispose()
        {
            this.localClient.OnBlockHeaders -= HandleBlockHeaders;
            this.coreDaemon.OnTargetChainChanged -= HandleTargetChainChanged;

            this.flushWorker.Dispose();
        }

        protected override void SubStart()
        {
            this.flushWorker.Start();
        }

        protected override void SubStop()
        {
            this.flushWorker.Stop();
        }

        protected override void WorkAction()
        {
            var now = DateTime.UtcNow;
            var requestTasks = new List<Task>();

            var peerCount = this.localClient.ConnectedPeers.Count;
            if (peerCount == 0)
                return;

            var targetChainLocal = this.coreDaemon.TargetChain;
            if (targetChainLocal == null)
                return;

            var blockLocatorHashes = CalculateBlockLocatorHashes(targetChainLocal.Blocks);

            // remove any stale requests from the peer's list of requests
            this.headersRequestsByPeer.RemoveWhere(x => (now - x.Value) > STALE_REQUEST_TIME);

            // loop through each connected peer
            var requestCount = 0;
            foreach (var peer in this.localClient.ConnectedPeers)
            {
                // determine if a new request can be made
                if (this.headersRequestsByPeer.TryAdd(peer, now))
                {
                    // send out the request for headers
                    requestTasks.Add(peer.Sender.SendGetHeaders(blockLocatorHashes, hashStop: 0));

                    // only send out a few header requests at a time
                    requestCount++;
                    if (requestCount >= 5)
                        break;
                }
            }
        }

        private void FlushWorkerMethod(WorkerMethod instance)
        {
            FlushHeaders flushHeaders;
            while (this.flushQueue.TryDequeue(out flushHeaders))
            {
                // cooperative loop
                this.ThrowIfCancelled();

                var peer = flushHeaders.Peer;
                var blockHeaders = flushHeaders.Headers;

                // chain the downloaded headers
                this.coreStorage.ChainHeaders(blockHeaders);

                DateTime ignore;
                this.headersRequestsByPeer.TryRemove(peer, out ignore);
            }
        }

        private void HandleBlockHeaders(Peer peer, IImmutableList<BlockHeader> blockHeaders)
        {
            if (blockHeaders.Count > 0)
            {
                this.flushQueue.Enqueue(new FlushHeaders(peer, blockHeaders));
                this.flushWorker.NotifyWork();
            }
            else
            {
                DateTime ignore;
                this.headersRequestsByPeer.TryRemove(peer, out ignore);
            }
        }

        private void HandleTargetChainChanged(object sender, EventArgs e)
        {
            this.NotifyWork();
        }

        private static ImmutableArray<UInt256> CalculateBlockLocatorHashes(IImmutableList<ChainedHeader> blockHashes)
        {
            var blockLocatorHashes = ImmutableArray.CreateBuilder<UInt256>();

            if (blockHashes.Count > 0)
            {
                var step = 1;
                var start = 0;
                for (var i = blockHashes.Count - 1; i > 0; i -= step, start++)
                {
                    if (start >= 10)
                        step *= 2;

                    blockLocatorHashes.Add(blockHashes[i].Hash);
                }
                blockLocatorHashes.Add(blockHashes[0].Hash);
            }

            return blockLocatorHashes.ToImmutable();
        }

        private sealed class FlushHeaders
        {
            private readonly Peer peer;
            private readonly IImmutableList<BlockHeader> headers;

            public FlushHeaders(Peer peer, IImmutableList<BlockHeader> headers)
            {
                this.peer = peer;
                this.headers = headers;
            }

            public Peer Peer { get { return this.peer; } }

            public IImmutableList<BlockHeader> Headers { get { return this.headers; } }
        }
    }
}
