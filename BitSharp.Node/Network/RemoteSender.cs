using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core;
using BitSharp.Core.Domain;
using BitSharp.Core.ExtensionMethods;
using BitSharp.Node.Domain;
using NLog;
using System;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Node.Network
{
    public class RemoteSender : IDisposable
    {
        public event Action<Exception> OnFailed;

        private readonly Logger logger;
        private readonly SemaphoreSlim semaphore = new SemaphoreSlim(1);
        private readonly Socket socket;

        public RemoteSender(Socket socket, Logger logger)
        {
            this.logger = logger;
            this.socket = socket;
        }

        public void Dispose()
        {
            this.semaphore.Dispose();
        }

        private void Fail(Exception e)
        {
            var handler = this.OnFailed;
            if (handler != null)
                handler(e);
        }

        public async Task RequestKnownAddressesAsync()
        {
            await SendMessageAsync("getaddr");
        }

        public async Task PingAsync()
        {
            await SendMessageAsync("ping");
        }

        public async Task SendBlock(Block block)
        {
            await Task.Yield();

            var sendBlockMessage = Messaging.ConstructMessage("block", DataEncoder.EncodeBlock(block));

            await SendMessageAsync(sendBlockMessage);
        }

        public async Task SendGetData(InventoryVector invVector)
        {
            await SendGetData(ImmutableArray.Create(invVector));
        }

        public async Task SendGetData(ImmutableArray<InventoryVector> invVectors)
        {
            await Task.Yield();

            var getDataPayload = Messaging.ConstructInventoryPayload(invVectors);
            var getDataMessage = Messaging.ConstructMessage("getdata", NodeEncoder.EncodeInventoryPayload(getDataPayload));

            await SendMessageAsync(getDataMessage);
        }

        public async Task SendGetHeaders(ImmutableArray<UInt256> blockLocatorHashes, UInt256 hashStop)
        {
            await Task.Yield();

            var getHeadersPayload = Messaging.ConstructGetBlocksPayload(blockLocatorHashes, hashStop);
            var getBlocksMessage = Messaging.ConstructMessage("getheaders", NodeEncoder.EncodeGetBlocksPayload(getHeadersPayload));

            await SendMessageAsync(getBlocksMessage);
        }

        public async Task SendGetBlocks(ImmutableArray<UInt256> blockLocatorHashes, UInt256 hashStop)
        {
            await Task.Yield();

            var getBlocksPayload = Messaging.ConstructGetBlocksPayload(blockLocatorHashes, hashStop);
            var getBlocksMessage = Messaging.ConstructMessage("getblocks", NodeEncoder.EncodeGetBlocksPayload(getBlocksPayload));

            await SendMessageAsync(getBlocksMessage);
        }

        public async Task SendHeaders(ImmutableArray<BlockHeader> blockHeaders)
        {
            await Task.Yield();

            using (var payloadStream = new MemoryStream())
            using (var payloadWriter = new BinaryWriter(payloadStream))
            {
                payloadWriter.WriteVarInt((UInt64)blockHeaders.Length);
                foreach (var blockHeader in blockHeaders)
                {
                    DataEncoder.EncodeBlockHeader(payloadStream, blockHeader);
                    payloadWriter.WriteVarInt(0);
                }

                await SendMessageAsync(Messaging.ConstructMessage("headers", payloadStream.ToArray()));
            }
        }

        public async Task SendInventory(ImmutableArray<InventoryVector> invVectors)
        {
            await Task.Yield();

            var invPayload = Messaging.ConstructInventoryPayload(invVectors);
            var invMessage = Messaging.ConstructMessage("inv", NodeEncoder.EncodeInventoryPayload(invPayload));

            await SendMessageAsync(invMessage);
        }

        public async Task SendTransaction(Transaction transaction)
        {
            await Task.Yield();

            var sendTxMessage = Messaging.ConstructMessage("tx", DataEncoder.EncodeTransaction(transaction));

            await SendMessageAsync(sendTxMessage);
        }

        public async Task SendVersion(IPEndPoint localEndPoint, IPEndPoint remoteEndPoint, UInt64 nodeId, UInt32 startBlockHeight)
        {
            await Task.Yield();

            var versionPayload = Messaging.ConstructVersionPayload(localEndPoint, remoteEndPoint, nodeId, startBlockHeight);
            var versionMessage = Messaging.ConstructMessage("version", NodeEncoder.EncodeVersionPayload(versionPayload, withRelay: false));

            await SendMessageAsync(versionMessage);
        }

        public async Task SendVersionAcknowledge()
        {
            await SendMessageAsync("verack");
        }

        public async Task SendMessageAsync(string command)
        {
            await SendMessageAsync(Messaging.ConstructMessage(command, payload: new byte[0]));
        }

        public async Task SendMessageAsync(Message message)
        {
            try
            {
                await semaphore.DoAsync(async () =>
                {
                    using (var stream = new NetworkStream(this.socket))
                    {
                        var stopwatch = Stopwatch.StartNew();

                        using (var byteStream = new MemoryStream())
                        {
                            NodeEncoder.EncodeMessage(byteStream, message);

                            var messageBytes = byteStream.ToArray();
                            await stream.WriteAsync(messageBytes, 0, messageBytes.Length);
                        }

                        stopwatch.Stop();

                        if (this.logger.IsTraceEnabled)
                            this.logger.Trace("Sent {0} in {1} ms\nPayload: {2}".Format2(message.Command, stopwatch.ElapsedMilliseconds, message.Payload.ToArray().ToHexDataString()));
                    }
                });
            }
            catch (Exception e)
            {
                Fail(e);
            }
        }
    }
}
