using BitSharp.Core.Monitor;
using System;
using System.Collections.Generic;

namespace BitSharp.Wallet
{
    public class MonitoredWalletAddress
    {
        private readonly IWalletAddress address;
        private readonly List<Tuple<ChainPosition, ChainPosition>> monitoredRanges;

        public MonitoredWalletAddress(IWalletAddress address, List<Tuple<ChainPosition, ChainPosition>> monitoredRanges)
        {
            this.address = address;
            this.monitoredRanges = monitoredRanges;
        }

        // address
        public IWalletAddress Address { get { return this.address; } }

        // ranges in the blockchain when monitoring was active
        public List<Tuple<ChainPosition, ChainPosition>> MonitoredRanges { get { return this.monitoredRanges; } }
    }
}
