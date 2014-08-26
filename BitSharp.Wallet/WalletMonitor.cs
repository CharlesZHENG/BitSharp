﻿using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using BitSharp.Core.Monitor;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BitSharp.Wallet
{
    //TODO the wallet is currently intimately tied to the chain state
    //TODO what do i do if the chain state commits before the wallet has had a chance to save its current state?
    //TODO i could potentially use the saved blocks & stored rollback information to replay any blocks that the wallet is behind on
    //TODO this would be useful to have in general, as wallets could then be turned off & on and then catch up properly

    public class WalletMonitor : Worker
    {
        private readonly Logger logger;
        private readonly CoreDaemon coreDaemon;
        private readonly BlockReplayer blockReplayer;

        private ChainBuilder chainBuilder;
        private int walletHeight;

        // addresses
        private readonly Dictionary<UInt256, List<MonitoredWalletAddress>> addressesByOutputScriptHash;
        private readonly List<MonitoredWalletAddress> matcherAddresses;

        // current point in the blockchain

        // entries
        private readonly ImmutableList<WalletEntry>.Builder entries;
        private readonly ReaderWriterLockSlim entriesLock;

        private decimal bitBalance;

        public WalletMonitor(CoreDaemon coreDaemon, Logger logger)
            : base("WalletMonitor", initialNotify: true, minIdleTime: TimeSpan.FromMilliseconds(100), maxIdleTime: TimeSpan.MaxValue, logger: logger)
        {
            this.logger = logger;
            this.coreDaemon = coreDaemon;
            this.blockReplayer = new BlockReplayer(coreDaemon.CoreStorage, coreDaemon.Rules, logger);

            this.chainBuilder = Chain.CreateForGenesisBlock(coreDaemon.Rules.GenesisChainedHeader).ToBuilder();

            this.addressesByOutputScriptHash = new Dictionary<UInt256, List<MonitoredWalletAddress>>();
            this.matcherAddresses = new List<MonitoredWalletAddress>();
            this.entries = ImmutableList.CreateBuilder<WalletEntry>();
            this.entriesLock = new ReaderWriterLockSlim();
            this.bitBalance = 0;

            this.coreDaemon.OnChainStateChanged += HandleChainStateChanged;
        }

        protected override void SubDispose()
        {
            this.coreDaemon.OnChainStateChanged -= HandleChainStateChanged;

            this.blockReplayer.Dispose();
        }

        public event Action OnScanned;

        public event Action<WalletEntry> OnEntryAdded;

        public IImmutableList<WalletEntry> Entries
        {
            get
            {
                return this.entriesLock.DoRead(() =>
                    this.entries.ToImmutable());
            }
        }

        public int WalletHeight
        {
            get { return this.walletHeight; }
        }

        public decimal BitBalance
        {
            get
            {
                return this.entriesLock.DoRead(() =>
                    this.bitBalance);
            }
        }

        public decimal BtcBalance
        {
            get { return this.BitBalance / 1.MILLION(); }
        }

        //TODO thread safety
        //TODO need to rescan utxo when addresses are added as well
        public void AddAddress(IWalletAddress address)
        {
            //TODO add to queue, cannot monitor address until chain position moves
            var startChainPosition = ChainPosition.Fake();
            var monitoredRange = new[] { Tuple.Create(startChainPosition, startChainPosition) }.ToList();

            foreach (var outputScriptHash in address.GetOutputScriptHashes())
            {
                List<MonitoredWalletAddress> addresses;
                if (!this.addressesByOutputScriptHash.TryGetValue(outputScriptHash, out addresses))
                {
                    addresses = new List<MonitoredWalletAddress>();
                    this.addressesByOutputScriptHash.Add(outputScriptHash, addresses);
                }

                addresses.Add(new MonitoredWalletAddress(address, monitoredRange));
            }

            if (address.IsMatcher)
            {
                this.matcherAddresses.Add(new MonitoredWalletAddress(address, monitoredRange));
            }
        }

        protected override void WorkAction()
        {
            using (var chainState = this.coreDaemon.GetChainState())
            {
                var stopwatch = Stopwatch.StartNew();
                foreach (var pathElement in this.chainBuilder.NavigateTowards(chainState.Chain))
                {
                    // cooperative loop
                    this.ThrowIfCancelled();

                    // get block and metadata for next link in blockchain
                    var direction = pathElement.Item1;
                    var chainedHeader = pathElement.Item2;
                    var forward = direction > 0;

                    try
                    {
                        ScanBlock(chainState, chainedHeader, forward);
                    }
                    catch (MissingDataException) {/*TODO no wallet state is saved, so missing data will be thrown when started up again due to pruning*/}
                    catch (AggregateException) {/*TODO no wallet state is saved, so missing data will be thrown when started up again due to pruning*/}

                    this.chainBuilder.AddBlock(chainedHeader);

                    this.walletHeight = chainedHeader.Height;
                    this.coreDaemon.PrunableHeight = this.walletHeight;

                    var handler = this.OnScanned;
                    if (handler != null)
                        handler();

                    // limit how long the chain state snapshot will be kept open
                    if (stopwatch.Elapsed > TimeSpan.FromSeconds(15))
                    {
                        this.NotifyWork();
                        return;
                    }
                }
            }
        }

        private void ScanBlock(IChainState chainState, ChainedHeader scanBlock, bool forward)
        {
            var sha256 = new SHA256Managed();

            using (this.blockReplayer.StartReplay(chainState, scanBlock.Hash))
            {
                foreach (var txWithPrevOutputs in this.blockReplayer.ReplayBlock())
                {
                    var tx = txWithPrevOutputs.Transaction;
                    var txIndex = txWithPrevOutputs.TxIndex;

                    if (txIndex > 0)
                    {
                        for (var inputIndex = 0; inputIndex < tx.Inputs.Length; inputIndex++)
                        {
                            var input = tx.Inputs[inputIndex];
                            var prevOutput = txWithPrevOutputs.PrevTxOutputs[inputIndex];
                            var prevOutputScriptHash = new UInt256(sha256.ComputeHash(prevOutput.ScriptPublicKey.ToArray()));

                            var chainPosition = ChainPosition.Fake();
                            var entryType = forward ? EnumWalletEntryType.Spend : EnumWalletEntryType.UnSpend;

                            ScanForEntry(chainPosition, entryType, prevOutput, prevOutputScriptHash);
                        }
                    }

                    for (var outputIndex = 0; outputIndex < tx.Outputs.Length; outputIndex++)
                    {
                        var output = tx.Outputs[outputIndex];
                        var outputScriptHash = new UInt256(sha256.ComputeHash(output.ScriptPublicKey.ToArray()));

                        var chainPosition = ChainPosition.Fake();
                        var entryType =
                            txIndex == 0 ?
                                (forward ? EnumWalletEntryType.Mine : EnumWalletEntryType.UnMine)
                                : (forward ? EnumWalletEntryType.Receive : EnumWalletEntryType.UnReceieve);

                        ScanForEntry(chainPosition, entryType, output, outputScriptHash);
                    }
                }
            }
        }

        private void ScanForEntry(ChainPosition chainPosition, EnumWalletEntryType walletEntryType, TxOutput txOutput, UInt256 outputScriptHash)
        {
            var matchingAddresses = ImmutableList.CreateBuilder<MonitoredWalletAddress>();

            // test hash addresses
            List<MonitoredWalletAddress> addresses;
            if (this.addressesByOutputScriptHash.TryGetValue(outputScriptHash, out addresses))
            {
                matchingAddresses.AddRange(addresses);
            }

            // test matcher addresses
            foreach (var address in this.matcherAddresses)
            {
                if (address.Address.MatchesTxOutput(txOutput, outputScriptHash))
                    matchingAddresses.Add(address);
            }

            if (matchingAddresses.Count > 0)
            {
                var entry = new WalletEntry
                (
                    addresses: matchingAddresses.ToImmutable(),
                    type: walletEntryType,
                    chainPosition: chainPosition,
                    value: txOutput.Value
                );

                this.entriesLock.DoWrite(() =>
                {
                    this.logger.Debug("{0,-10}   {1,20:#,##0.000_000_00} BTC, Entries: {2:#,##0}".Format2(walletEntryType.ToString() + ":", txOutput.Value / (decimal)(100.MILLION()), this.entries.Count));

                    this.entries.Add(entry);
                    this.bitBalance += entry.BitValue * walletEntryType.Direction();
                });

                var handler = this.OnEntryAdded;
                if (handler != null)
                    handler(entry);
            }
        }

        private void HandleChainStateChanged(object sender, EventArgs e)
        {
            this.NotifyWork();
        }
    }
}
