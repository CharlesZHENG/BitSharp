using BitSharp.Core.Monitor;
using System;
using System.Collections.Generic;

namespace BitSharp.Wallet
{
    public class MonitoredWalletAddress
    {
        public MonitoredWalletAddress(IWalletAddress address, List<Tuple<ChainPosition, ChainPosition>> monitoredRanges)
        {
            Address = address;
            MonitoredRanges = monitoredRanges;
        }

        // address
        public IWalletAddress Address { get; }

        // ranges in the blockchain when monitoring was active
        public List<Tuple<ChainPosition, ChainPosition>> MonitoredRanges { get; }
    }
}
