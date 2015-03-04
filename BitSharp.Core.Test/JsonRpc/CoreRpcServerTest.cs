using BitSharp.Core.JsonRpc;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using NLog;
using System;
using System.IO;
using System.Net;
using System.Text;

namespace BitSharp.Core.Test.JsonRpc
{
    [TestClass]
    public class CoreRpcServerTest
    {
        [TestMethod]
        public void TestRpcGetBlockCount()
        {
            using (var simulator = new MainnetSimulator())
            {
                var logger = LogManager.GetCurrentClassLogger();
                using (var rpcServer = new CoreRpcServer(simulator.CoreDaemon))
                {
                    rpcServer.StartListening();

                    var block9 = simulator.BlockProvider.GetBlock(9);

                    simulator.AddBlockRange(0, 9);
                    simulator.WaitForUpdate();
                    AssertMethods.AssertDaemonAtBlock(9, block9.Hash, simulator.CoreDaemon);

                    var jsonRequestId = Guid.NewGuid().ToString();
                    var jsonRequest = JsonConvert.SerializeObject(
                        new JsonRpcRequest
                        {
                            method = "getblockcount",
                            @params = new string[0],
                            id = jsonRequestId
                        });
                    var jsonRequestBytes = Encoding.UTF8.GetBytes(jsonRequest);

                    var request = (HttpWebRequest)WebRequest.Create("http://localhost:8332");
                    request.Method = WebRequestMethods.Http.Post;
                    using (var requestStream = request.GetRequestStream())
                    {
                        requestStream.Write(jsonRequestBytes, 0, jsonRequestBytes.Length);
                    }

                    using (var response = request.GetResponse())
                    using (var responseStream = response.GetResponseStream())
                    using (var responseStreamReader = new StreamReader(responseStream, Encoding.UTF8))
                    {
                        var jsonResponseString = responseStreamReader.ReadToEnd();
                        var jsonResponse = JsonConvert.DeserializeObject<JsonRpcResponse>(jsonResponseString);

                        Assert.AreEqual("2.0", jsonResponse.jsonrpc);
                        Assert.AreEqual("9", jsonResponse.result);
                        Assert.AreEqual(jsonRequestId, jsonResponse.id);
                    }
                }
            }
        }

        private sealed class JsonRpcRequest
        {
            public string method { get; set; }
            public string[] @params { get; set; }
            public string id { get; set; }
        }

        private sealed class JsonRpcResponse
        {
            public string jsonrpc { get; set; }
            public string result { get; set; }
            public string id { get; set; }
        }
    }
}
