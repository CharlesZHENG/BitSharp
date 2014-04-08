﻿using BitSharp.Core.Domain;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.BlockHelper
{
    public class BlockExplorerProvider
    {
        private static readonly Regex hashRegex = new Regex("http://blockexplorer.com/block/([0-9A-Fa-f]{64})");
        private static readonly double REQUESTS_PER_MINUTE = 60 * 400;
        private static readonly TimeSpan THROTTLE_DELAY = TimeSpan.FromMinutes(1 / REQUESTS_PER_MINUTE);

        //TODO not thread safe
        private static DateTime lastDownload;

        public Block GetBlock(int index)
        {
            return BlockJson.GetBlockFromJson(GetBlockJson(index));
        }

        public Block GetBlock(string hash)
        {
            return BlockJson.GetBlockFromJson(GetBlockJson(hash));
        }

        public IEnumerable<Block> GetBlocks(IEnumerable<int> blockIndexes)
        {
            var enumerator = blockIndexes.GetEnumerator();

            // move to the first block index
            if (!enumerator.MoveNext())
                yield break;

            // load up the first block to be returned
            var block = GetBlock(enumerator.Current);

            while (enumerator.MoveNext())
            {
                var nextBlockTask = Task.Run(() => GetBlock(enumerator.Current));
                yield return block;
                nextBlockTask.Wait();
                block = nextBlockTask.Result;
            }

            yield return block;
        }

        public string GetBlockJson(int index)
        {
            return GetBlockJson(GetBlockHash(index));
        }

        public string GetBlockJson(string hash)
        {
            Throttle();

            var url = new Uri(string.Format("http://blockexplorer.com/rawblock/{0}", hash));
            using (var client = new WebClient())
            {
                return client.DownloadString(url);
            }
        }

        public string GetBlockHash(int index)
        {
            Throttle();

            var url = new Uri(string.Format("http://blockexplorer.com/b/{0}", index));
            using (var client = new HttpClient())
            {
                var request = WebRequest.CreateHttp(url);
                request.AllowAutoRedirect = false;

                using (var response = request.GetResponse())
                {
                    var redirectUrl = response.Headers["Location"];
                    var hash = hashRegex.Match(redirectUrl).Groups[1].Value;

                    return hash;
                }
            }
        }

        private static void Throttle()
        {
            var now = DateTime.UtcNow;
            var delta = now - lastDownload;
            if (delta < THROTTLE_DELAY)
            {
                var wait = THROTTLE_DELAY - delta;
                Thread.Sleep((int)wait.TotalMilliseconds);
            }

            lastDownload = DateTime.UtcNow;
        }
    }
}
