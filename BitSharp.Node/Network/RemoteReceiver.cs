﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Common;
using BitSharp.Core.ExtensionMethods;
using System.Collections.Immutable;
using NLog;
using BitSharp.Node.Domain;
using BitSharp.Core.Domain;
using BitSharp.Core;

namespace BitSharp.Node.Network
{
    public class RemoteReceiver
    {
        public event Action<Exception> OnFailed;
        public event Action<Message> OnMessage;
        public event Action<VersionPayload> OnVersion;
        public event Action OnVersionAcknowledged;
        public event Action<ImmutableArray<InventoryVector>> OnInventoryVectors;
        public event Action<ImmutableArray<InventoryVector>> OnNotFound;
        public event Action<RemoteNode, Block> OnBlock;
        public event Action<RemoteNode, IImmutableList<BlockHeader>> OnBlockHeaders;
        public event Action<Transaction> OnTransaction;
        public event Action<ImmutableArray<NetworkAddressWithTime>> OnReceivedAddresses;
        public event Action<GetBlocksPayload> OnGetBlocks;
        public event Action<GetBlocksPayload> OnGetHeaders;
        public event Action<InventoryPayload> OnGetData;
        public event Action<ImmutableArray<byte>> OnPing;

        private readonly Logger logger;
        private readonly RemoteNode owner;
        private readonly Socket socket;
        private readonly bool persistent;

        public RemoteReceiver(RemoteNode owner, Socket socket, bool persistent, Logger logger)
        {
            this.logger = logger;
            this.owner = owner;
            this.socket = socket;
            this.persistent = persistent;
        }

        private void Fail(Exception e)
        {
            var handler = this.OnFailed;
            if (handler != null)
                handler(e);
        }

        public void Listen()
        {
            Task.Factory.StartNew(async () =>
            {
                try
                {
                    while (true)
                    {
                        var buffer = new byte[4];
                        var bytesReceived = await Task.Factory.FromAsync<int>(this.socket.BeginReceive(buffer, 0, 4, SocketFlags.None, null, null), this.socket.EndReceive);

                        HandleMessage(buffer, bytesReceived);
                    }
                }
                catch (Exception e)
                {
                    Fail(e);
                }
            }, TaskCreationOptions.LongRunning);
        }

        public async Task<Message> WaitForMessage(Func<Message, bool> predicate, int timeoutMilliseconds)
        {
            return await WaitForMessage(predicate, TimeSpan.FromMilliseconds(timeoutMilliseconds));
        }

        public async Task<Message> WaitForMessage(Func<Message, bool> predicate, TimeSpan timeout)
        {
            var messageTcs = new TaskCompletionSource<Message>();
            Action<Message> handler =
                message =>
                {
                    if (predicate(message))
                        messageTcs.SetResult(message);
                };

            this.OnMessage += handler;
            try
            {
                if (await Task.WhenAny(messageTcs.Task, Task.Delay(timeout)) == messageTcs.Task)
                {
                    return await messageTcs.Task;
                }
                else
                {
                    throw new TimeoutException();
                }
            }
            finally
            {
                this.OnMessage -= handler;
            }
        }

        private void HandleMessage(byte[] buffer, int bytesReceived)
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            if (bytesReceived == 0)
            {
                Thread.Sleep(10);
                return;
            }
            else if (bytesReceived < 4)
            {
                using (var stream = new NetworkStream(this.socket))
                using (var reader = new BinaryReader(stream))
                {
                    Buffer.BlockCopy(reader.ReadBytes(4 - bytesReceived), 0, buffer, bytesReceived, 4 - bytesReceived);
                }
            }

            var magic = Bits.ToUInt32(buffer);
            if (magic != Messaging.Magic)
                throw new Exception(string.Format("Unknown magic bytes {0}", buffer.ToHexNumberString()));

            using (var stream = new NetworkStream(this.socket))
            {
                var message = WireDecodeMessage(magic, stream);

                var handler = this.OnMessage;
                if (handler != null)
                    handler(message);

                stopwatch.Stop();
                
                if (this.logger.IsTraceEnabled)
                    this.logger.Trace("{2,25} Received message {0,12} in {1,6} ms".Format2(message.Command, stopwatch.ElapsedMilliseconds, this.socket.RemoteEndPoint));
            }
        }

        private Message WireDecodeMessage(UInt32 magic, Stream stream)
        {
            byte[] payload;
            Message message;
            using (var reader = new BinaryReader(stream, Encoding.ASCII, leaveOpen: true))
            {
                var command = reader.ReadFixedString(12);
                var payloadSize = reader.ReadUInt32();
                var payloadChecksum = reader.ReadUInt32();

                payload = reader.ReadBytes(payloadSize.ToIntChecked());

                if (!Messaging.VerifyPayloadChecksum(payloadChecksum, payload))
                    throw new Exception(string.Format("Checksum failed for {0}", command));

                message = new Message
                (
                    Magic: magic,
                    Command: command,
                    PayloadSize: payloadSize,
                    PayloadChecksum: payloadChecksum,
                    Payload: payload.ToImmutableArray()
                );
            }

            switch (message.Command)
            {
                case "addr":
                    {
                        var addressPayload = NodeEncoder.DecodeAddressPayload(payload);

                        var handler = this.OnReceivedAddresses;
                        if (handler != null)
                            handler(addressPayload.NetworkAddresses);
                    }
                    break;

                case "alert":
                    {
                        var alertPayload = NodeEncoder.DecodeAlertPayload(payload);
                    }
                    break;

                case "block":
                    {
                        var block = DataEncoder.DecodeBlock(payload);

                        var handler = this.OnBlock;
                        if (handler != null)
                            handler(this.owner, block);
                    }
                    break;

                case "getblocks":
                    {
                        var getBlocksPayload = NodeEncoder.DecodeGetBlocksPayload(payload);

                        var handler = this.OnGetBlocks;
                        if (handler != null)
                            handler(getBlocksPayload);
                    }
                    break;

                case "getheaders":
                    {
                        var getHeadersPayload = NodeEncoder.DecodeGetBlocksPayload(payload);

                        var handler = this.OnGetHeaders;
                        if (handler != null)
                            handler(getHeadersPayload);
                    }
                    break;

                case "getdata":
                    {
                        var invPayload = NodeEncoder.DecodeInventoryPayload(payload);

                        var handler = this.OnGetData;
                        if (handler != null)
                            handler(invPayload);
                    }
                    break;

                case "headers":
                    {
                        var blockHeaders = ImmutableList.CreateBuilder<BlockHeader>();

                        using (var headerStream = new MemoryStream(payload))
                        using (var reader = new BinaryReader(headerStream))
                        {
                            var headerCount = reader.ReadVarInt().ToIntChecked();

                            for (var i = 0; i < headerCount; i++)
                            {
                                var blockHeader = DataEncoder.DecodeBlockHeader(headerStream);
                                //TODO wiki says this is a byte and a var int, which is it?
                                var txCount = reader.ReadVarInt();

                                blockHeaders.Add(blockHeader);
                            }
                        }

                        var handler = this.OnBlockHeaders;
                        if (handler != null)
                            handler(this.owner, blockHeaders.ToImmutable());
                    }
                    break;

                case "inv":
                    {
                        var invPayload = NodeEncoder.DecodeInventoryPayload(payload);

                        var handler = this.OnInventoryVectors;
                        if (handler != null)
                            handler(invPayload.InventoryVectors);
                    }
                    break;

                case "notfound":
                    {
                        var invPayload = NodeEncoder.DecodeInventoryPayload(payload);

                        var handler = this.OnNotFound;
                        if (handler != null)
                            handler(invPayload.InventoryVectors);
                    }
                    break;

                case "ping":
                    {
                        var handler = this.OnPing;
                        if (handler != null)
                            handler(payload.ToImmutableArray());
                    }
                    break;

                case "tx":
                    {
                        var tx = DataEncoder.DecodeTransaction(payload);

                        var handler = this.OnTransaction;
                        if (handler != null)
                            handler(tx);
                    }
                    break;

                case "version":
                    {
                        var versionPayload = NodeEncoder.DecodeVersionPayload(payload, payload.Length);

                        var handler = this.OnVersion;
                        if (handler != null)
                            handler(versionPayload);
                    }
                    break;

                case "verack":
                    {
                        var handler = this.OnVersionAcknowledged;
                        if (handler != null)
                            handler();
                    }
                    break;

                default:
                    {
                        this.logger.Warn("Unhandled incoming message: {0}".Format2(message.Command));
                    }
                    break;
            }

            //TODO
            //if (payloadStream.Position != payloadStream.Length)
            //{
            //    var exMessage = string.Format("Wrong number of bytes read for {0}, parser error: read {1} bytes from a {2} byte payload", message.Command, payloadStream.Position, payloadStream.Length);
            //    Debug.WriteLine(exMessage);
            //    throw new Exception(exMessage);
            //}

            return message;
        }
    }
}
