﻿using AustinHarris.JsonRpc;
using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core;
using NLog;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace BitSharp.Core.JsonRpc
{
    //TODO the reference implementation has the chain information, network information, and wallet information all running under one RPC service
    //TODO i'm not touching private keys, so all of the wallet commands will be for monitoring
    //TODO i'll have to add something non-standard to tell it what addresses to watch, so i can use standard commands like "getreceivedbyaddress"
    public class CoreRpcServer : JsonRpcService, IDisposable
    {
        private readonly Logger logger;
        private readonly CoreDaemon coreDaemon;
        private readonly ListenerWorker listener;

        public CoreRpcServer(Logger logger, CoreDaemon coreDaemon)
        {
            this.logger = logger;
            this.coreDaemon = coreDaemon;
            this.listener = new ListenerWorker(this, this.logger);
        }

        public void Dispose()
        {
            this.listener.Dispose();
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
            private readonly Logger logger;
            private readonly CoreRpcServer rpcServer;
            private HttpListener httpListener;

            public ListenerWorker(CoreRpcServer rpcServer, Logger logger)
                : base("CoreRpcServer.ListenerWorker", initialNotify: true, minIdleTime: TimeSpan.Zero, maxIdleTime: TimeSpan.Zero, logger: logger)
            {
                this.logger = logger;
                this.rpcServer = rpcServer;
            }

            protected override void SubStart()
            {
                if (this.httpListener == null)
                {
                    this.httpListener = new HttpListener();
                    this.httpListener.Prefixes.Add("http://localhost:8332/");
                }

                this.httpListener.Start();
            }

            protected override void SubStop()
            {
                if (this.httpListener != null)
                    this.httpListener.Stop();
            }

            protected override void WorkAction()
            {
                try
                {
                    var context = this.httpListener.GetContext();

                    var reader = new StreamReader(context.Request.InputStream, Encoding.UTF8);
                    var line = reader.ReadToEnd();
                    
                    var async = new JsonRpcStateAsync(RpcResultHandler, context.Response) { JsonRpc = line };
                    JsonRpcProcessor.Process(async, this.rpcServer);
                }
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

                Debug.WriteLine("result: {0}".Format2(result));

                var resultBytes = Encoding.UTF8.GetBytes(result);

                response.ContentType = "application/json";
                response.ContentEncoding = Encoding.UTF8;

                response.ContentLength64 = resultBytes.Length;
                response.OutputStream.Write(resultBytes, 0, resultBytes.Length);
            }
        }
    }
}
