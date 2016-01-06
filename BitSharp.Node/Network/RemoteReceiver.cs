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
        public event Action<Peer, Exception> OnFailed;
        public event Action<Peer, Message> OnMessage;
        public event Action<Peer, VersionPayload> OnVersion;
        public event Action<Peer> OnVersionAcknowledged;
        public event Action<Peer, ImmutableArray<InventoryVector>> OnInventoryVectors;
        public event Action<Peer, ImmutableArray<InventoryVector>> OnNotFound;
        public event Action<Peer, Block> OnBlock;
        public event Action<Peer, IImmutableList<BlockHeader>> OnBlockHeaders;
        public event Action<Peer, DecodedTx> OnTransaction;
        public event Action<Peer, ImmutableArray<NetworkAddressWithTime>> OnReceivedAddresses;
        public event Action<Peer, GetBlocksPayload> OnGetBlocks;
        public event Action<Peer, GetBlocksPayload> OnGetHeaders;
        public event Action<Peer, InventoryPayload> OnGetData;
        public event Action<Peer, ImmutableArray<byte>> OnPing;

        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly Peer owner;
        private readonly Socket socket;

        public RemoteReceiver(Peer owner, Socket socket)
        {
            this.owner = owner;
            this.socket = socket;
        }

        private void Fail(Exception e)
        {
            OnFailed?.Invoke(owner, e);
        }

        public void Listen()
        {
            Task.Factory.StartNew(async () =>
            {
                try
                {
                    while (true)
                    {
                        var messageStart = await ReceiveExactly(4);
                        await HandleMessage(messageStart);
                    }
                }
                catch (Exception ex)
                {
                    if (!(ex is ObjectDisposedException))
                        logger.Error(ex, "Peer failed handling message.");

                    Fail(ex);
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
            Action<Peer, Message> handler =
                (_, message) =>
                {
                    if (predicate(message))
                        messageTcs.SetResult(message);
                };

            OnMessage += handler;
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
                OnMessage -= handler;
            }
        }

        private async Task HandleMessage(byte[] messageStart)
        {
            var stopwatch = Stopwatch.StartNew();

            var magic = Bits.ToUInt32(messageStart);
            if (magic != Messaging.Magic)
                throw new Exception($"Unknown magic bytes {messageStart.ToHexNumberString()}");

            var message = await WireDecodeMessage(magic);

            OnMessage?.Invoke(owner, message);

            stopwatch.Stop();

            if (logger.IsTraceEnabled)
                logger.Trace($"{socket.RemoteEndPoint,25} Received message {message.Command,12} in {stopwatch.ElapsedMilliseconds,6} ms");
        }

        private async Task<Message> WireDecodeMessage(UInt32 magic)
        {
            var command = DataDecoder.DecodeFixedString(await ReceiveExactly(12));
            var payloadSize = DataDecoder.DecodeUInt32(await ReceiveExactly(4));
            var payloadChecksum = DataDecoder.DecodeUInt32(await ReceiveExactly(4));
            var payload = await ReceiveExactly(payloadSize.ToIntChecked());

            if (!Messaging.VerifyPayloadChecksum(payloadChecksum, payload))
                throw new Exception($"Checksum failed for {command}");

            var message = new Message
            (
                Magic: magic,
                Command: command,
                PayloadSize: payloadSize,
                PayloadChecksum: payloadChecksum,
                Payload: payload.ToImmutableArray()
            );

            switch (message.Command)
            {
                case "addr":
                    {
                        var addressPayload = NodeEncoder.DecodeAddressPayload(payload);

                        OnReceivedAddresses?.Invoke(owner, addressPayload.NetworkAddresses);
                    }
                    break;

                case "alert":
                    {
                        var alertPayload = NodeEncoder.DecodeAlertPayload(payload);
                    }
                    break;

                case "block":
                    {
                        var block = DataDecoder.DecodeBlock(null, payload);

                        OnBlock?.Invoke(owner, block);
                    }
                    break;

                case "getblocks":
                    {
                        var getBlocksPayload = NodeEncoder.DecodeGetBlocksPayload(payload);

                        OnGetBlocks?.Invoke(owner, getBlocksPayload);
                    }
                    break;

                case "getheaders":
                    {
                        var getHeadersPayload = NodeEncoder.DecodeGetBlocksPayload(payload);

                        OnGetHeaders?.Invoke(owner, getHeadersPayload);
                    }
                    break;

                case "getdata":
                    {
                        var invPayload = NodeEncoder.DecodeInventoryPayload(payload);

                        OnGetData?.Invoke(owner, invPayload);
                    }
                    break;

                case "headers":
                    {
                        var blockHeaders = ImmutableList.CreateBuilder<BlockHeader>();

                        var offset = 0;
                        var headerCount = payload.ReadVarInt(ref offset).ToIntChecked();

                        for (var i = 0; i < headerCount; i++)
                        {
                            var blockHeader = DataDecoder.DecodeBlockHeader(null, payload, ref offset);
                            // ignore tx count var int
                            payload.ReadVarInt(ref offset);

                            blockHeaders.Add(blockHeader);
                        }

                        OnBlockHeaders?.Invoke(owner, blockHeaders.ToImmutable());
                    }
                    break;

                case "inv":
                    {
                        var invPayload = NodeEncoder.DecodeInventoryPayload(payload);

                        OnInventoryVectors?.Invoke(owner, invPayload.InventoryVectors);
                    }
                    break;

                case "notfound":
                    {
                        var invPayload = NodeEncoder.DecodeInventoryPayload(payload);

                        OnNotFound?.Invoke(owner, invPayload.InventoryVectors);
                    }
                    break;

                case "ping":
                    {
                        OnPing?.Invoke(owner, payload.ToImmutableArray());
                    }
                    break;

                case "tx":
                    {
                        var tx = DataDecoder.DecodeEncodedTx(null, payload);

                        OnTransaction?.Invoke(owner, tx);
                    }
                    break;

                case "version":
                    {
                        var versionPayload = NodeEncoder.DecodeVersionPayload(payload, payload.Length);

                        OnVersion?.Invoke(owner, versionPayload);
                    }
                    break;

                case "verack":
                    {
                        OnVersionAcknowledged?.Invoke(owner);
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

        private async Task<byte[]> ReceiveExactly(int count)
        {
            var buffer = new byte[count];
            if (count == 0)
                return buffer;

            var readCount = 0;
            while (readCount < count)
            {
                var remainingCount = count - readCount;

                readCount += await Task.Factory.FromAsync<int>(
                    socket.BeginReceive(buffer, readCount, remainingCount, SocketFlags.None, null, null), socket.EndReceive);
            }

            if (readCount != count)
                throw new InvalidOperationException();

            return buffer;
        }
    }
}
