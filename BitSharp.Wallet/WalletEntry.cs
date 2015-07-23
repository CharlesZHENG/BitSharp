using BitSharp.Common.ExtensionMethods;
using BitSharp.Core.Monitor;
using System;
using System.Collections.Immutable;

namespace BitSharp.Wallet
{
    public class WalletEntry
    {
        public WalletEntry(IImmutableList<MonitoredWalletAddress> addresses, EnumWalletEntryType type, ChainPosition chainPosition, UInt64 value)
        {
            Addresses = addresses;
            Type = type;
            ChainPosition = chainPosition;
            Value = value;
        }

        public IImmutableList<MonitoredWalletAddress> Addresses { get; }

        public EnumWalletEntryType Type { get; }

        public ChainPosition ChainPosition { get; }

        public UInt64 Value { get; }

        public decimal BtcValue => this.Value / 100m.MILLION();

        public decimal BitValue => this.Value / 100m;
    }
}
