using AustinHarris.JsonRpc;
using BitSharp.Common;
using NLog;
using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace BitSharp.Core.JsonRpc
{
    //TODO the reference implementation has the chain information, network information, and wallet information all running under one RPC service
    //TODO i'm not touching private keys, so all of the wallet commands will be for monitoring
    //TODO i'll have to add something non-standard to tell it what addresses to watch, so i can use standard commands like "getreceivedbyaddress"
    public class CoreRpcServer : JsonRpcService, IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly CoreDaemon coreDaemon;
        private readonly ListenerWorker listener;

        private bool isDisposed;

        public CoreRpcServer(CoreDaemon coreDaemon, int port)
        {
            this.coreDaemon = coreDaemon;
            this.listener = new ListenerWorker(this, port);
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
                this.listener.Dispose();

                isDisposed = true;
            }
        }

        public void StartListening()
        {
            this.listener.Start();
        }

        public void StopListening()
        {
            this.listener.Stop();
        }

        [JsonRpcMethod("getblock")]
        public void GetBlock(UInt256 blockHash)
        {
        }

        [JsonRpcMethod("getblockcount")]
        public int GetBlockCount()
        {
            return this.coreDaemon.CurrentChain.Height;
        }

        [JsonRpcMethod("getreceivedbyaddress")]
        public void GetReceivedByAddress(string address, int minConf)
        {
        }

        private sealed class ListenerWorker : Worker
        {
            private static readonly Logger logger = LogManager.GetCurrentClassLogger();

            private readonly CoreRpcServer rpcServer;
            private readonly int port;
            private HttpListener httpListener;

            public ListenerWorker(CoreRpcServer rpcServer, int port)
                : base("CoreRpcServer.ListenerWorker", initialNotify: true, minIdleTime: TimeSpan.Zero, maxIdleTime: TimeSpan.Zero)
            {
                this.rpcServer = rpcServer;
                this.port = port;
            }

            protected override void SubStart()
            {
                if (this.httpListener == null)
                {
                    this.httpListener = new HttpListener();
                    this.httpListener.Prefixes.Add($"http://localhost:{port}/");
                }

                this.httpListener.Start();
            }

            protected override void SubStop()
            {
                this.httpListener?.Stop();
            }

            protected override async Task WorkAction()
            {
                try
                {
                    var context = await this.httpListener.GetContextAsync();

                    var reader = new StreamReader(context.Request.InputStream, Encoding.UTF8);
                    var line = await reader.ReadToEndAsync();

                    var async = new JsonRpcStateAsync(RpcResultHandler, context.Response) { JsonRpc = line };
                    JsonRpcProcessor.Process(async, this.rpcServer);
                }
                // ignore the exception if the worker is stopped
                // HttpListenerException will be thrown on SubStop
                catch (HttpListenerException) when (!this.IsStarted) { }
                finally
                {
                    // always notify to continue accepting connections
                    this.NotifyWork();
                }
            }

            private void RpcResultHandler(IAsyncResult state)
            {
                var async = ((JsonRpcStateAsync)state);
                var result = async.Result;
                var response = ((HttpListenerResponse)async.AsyncState);

                Debug.WriteLine($"result: {result}");

                var resultBytes = Encoding.UTF8.GetBytes(result);

                response.ContentType = "application/json";
                response.ContentEncoding = Encoding.UTF8;

                response.ContentLength64 = resultBytes.Length;
                response.OutputStream.Write(resultBytes, 0, resultBytes.Length);
            }
        }
    }
}
