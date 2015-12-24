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
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

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
        public event Action<Peer, Block> OnBlock;
        public event Action<Peer, IImmutableList<BlockHeader>> OnBlockHeaders;
        public event Action<Transaction> OnTransaction;
        public event Action<ImmutableArray<NetworkAddressWithTime>> OnReceivedAddresses;
        public event Action<GetBlocksPayload> OnGetBlocks;
        public event Action<GetBlocksPayload> OnGetHeaders;
        public event Action<InventoryPayload> OnGetData;
        public event Action<ImmutableArray<byte>> OnPing;

        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly Peer owner;
        private readonly Socket socket;
        private readonly bool persistent;

        public RemoteReceiver(Peer owner, Socket socket, bool persistent)
        {
            this.owner = owner;
            this.socket = socket;
            this.persistent = persistent;
        }

        private void Fail(Exception e)
        {
            this.OnFailed?.Invoke(e);
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
            var stopwatch = Stopwatch.StartNew();

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
                throw new Exception($"Unknown magic bytes {buffer.ToHexNumberString()}");

            using (var stream = new NetworkStream(this.socket))
            {
                var message = WireDecodeMessage(magic, stream);

                this.OnMessage?.Invoke(message);

                stopwatch.Stop();

                if (logger.IsTraceEnabled)
                    logger.Trace($"{this.socket.RemoteEndPoint,25} Received message {message.Command,12} in {stopwatch.ElapsedMilliseconds,6} ms");
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
                    throw new Exception($"Checksum failed for {command}");

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

                        this.OnReceivedAddresses?.Invoke(addressPayload.NetworkAddresses);
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

                        this.OnBlock?.Invoke(this.owner, block);
                    }
                    break;

                case "getblocks":
                    {
                        var getBlocksPayload = NodeEncoder.DecodeGetBlocksPayload(payload);

                        this.OnGetBlocks?.Invoke(getBlocksPayload);
                    }
                    break;

                case "getheaders":
                    {
                        var getHeadersPayload = NodeEncoder.DecodeGetBlocksPayload(payload);

                        this.OnGetHeaders?.Invoke(getHeadersPayload);
                    }
                    break;

                case "getdata":
                    {
                        var invPayload = NodeEncoder.DecodeInventoryPayload(payload);

                        this.OnGetData?.Invoke(invPayload);
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
                                var blockHeader = DataEncoder.DecodeBlockHeader(reader);
                                //TODO wiki says this is a byte and a var int, which is it?
                                var txCount = reader.ReadVarInt();

                                blockHeaders.Add(blockHeader);
                            }
                        }

                        this.OnBlockHeaders?.Invoke(this.owner, blockHeaders.ToImmutable());
                    }
                    break;

                case "inv":
                    {
                        var invPayload = NodeEncoder.DecodeInventoryPayload(payload);

                        this.OnInventoryVectors?.Invoke(invPayload.InventoryVectors);
                    }
                    break;

                case "notfound":
                    {
                        var invPayload = NodeEncoder.DecodeInventoryPayload(payload);

                        this.OnNotFound?.Invoke(invPayload.InventoryVectors);
                    }
                    break;

                case "ping":
                    {
                        this.OnPing?.Invoke(payload.ToImmutableArray());
                    }
                    break;

                case "tx":
                    {
                        var tx = DataEncoder.DecodeTransaction(payload).Decode();

                        this.OnTransaction?.Invoke(tx);
                    }
                    break;

                case "version":
                    {
                        var versionPayload = NodeEncoder.DecodeVersionPayload(payload, payload.Length);

                        this.OnVersion?.Invoke(versionPayload);
                    }
                    break;

                case "verack":
                    {
                        this.OnVersionAcknowledged?.Invoke();
                    }
                    break;

                default:
                    {
                        logger.Warn($"Unhandled incoming message: {message.Command}");
                    }
                    break;
            }

            //TODO
            //if (payloadStream.Position != payloadStream.Length)
            //{
            //    var exMessage = $"Wrong number of bytes read for {message.Command}, parser error: read {payloadStream.Position} bytes from a {payloadStream.Length} byte payload";
            //    Debug.WriteLine(exMessage);
            //    throw new Exception(exMessage);
            //}

            return message;
        }
    }
}
