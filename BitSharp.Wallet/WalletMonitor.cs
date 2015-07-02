using BitSharp.Common;
using BitSharp.Common.ExtensionMethods;
using BitSharp.Core;
using BitSharp.Core.Builders;
using BitSharp.Core.Domain;
using BitSharp.Core.Monitor;
using BitSharp.Core.Storage;
using NLog;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace BitSharp.Wallet
{
    public class WalletMonitor : Worker
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly CoreDaemon coreDaemon;

        private ChainBuilder chainBuilder;
        private int walletHeight;

        private readonly UpdatedTracker updatedTracker = new UpdatedTracker();

        // addresses
        private readonly Dictionary<UInt256, List<MonitoredWalletAddress>> addressesByOutputScriptHash;
        private readonly List<MonitoredWalletAddress> matcherAddresses;

        // current point in the blockchain

        // entries
        private readonly ImmutableList<WalletEntry>.Builder entries;
        private readonly ReaderWriterLockSlim entriesLock;

        private decimal bitBalance;

        public WalletMonitor(CoreDaemon coreDaemon)
            : base("WalletMonitor", initialNotify: true, minIdleTime: TimeSpan.FromMilliseconds(100), maxIdleTime: TimeSpan.MaxValue)
        {
            this.coreDaemon = coreDaemon;

            //this.chainBuilder = Chain.CreateForGenesisBlock(coreDaemon.Rules.GenesisChainedHeader).ToBuilder();
            //TODO start from the currently processed chain tip since wallet state isn't persisted
            this.chainBuilder = coreDaemon.CurrentChain.ToBuilder();

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

        public void WaitForUpdate()
        {
            this.updatedTracker.WaitForUpdate();
        }

        public bool WaitForUpdate(TimeSpan timeout)
        {
            return this.updatedTracker.WaitForUpdate(timeout);
        }

        protected override void WorkAction()
        {
            using (updatedTracker.TryUpdate(staleAction: NotifyWork))
            using (var chainState = this.coreDaemon.GetChainState())
            {
                var stopwatch = Stopwatch.StartNew();
                foreach (var pathElement in this.chainBuilder.ToImmutable().NavigateTowards(chainState.Chain))
                {
                    // cooperative loop
                    this.ThrowIfCancelled();

                    // get block and metadata for next link in blockchain
                    var direction = pathElement.Item1;
                    var chainedHeader = pathElement.Item2;
                    var forward = direction > 0;

                    try
                    {
                        ScanBlock(coreDaemon.CoreStorage, chainState, chainedHeader, forward);
                    }
                    catch (MissingDataException) {/*TODO no wallet state is saved, so missing data will be thrown when started up again due to pruning*/}
                    catch (AggregateException) {/*TODO no wallet state is saved, so missing data will be thrown when started up again due to pruning*/}

                    if (forward)
                        this.chainBuilder.AddBlock(chainedHeader);
                    else
                        this.chainBuilder.RemoveBlock(chainedHeader);

                    this.walletHeight = this.chainBuilder.Height;
                    this.coreDaemon.PrunableHeight = this.walletHeight;

                    var handler = this.OnScanned;
                    if (handler != null)
                        handler();

                    // limit how long the chain state snapshot will be kept open
                    if (stopwatch.Elapsed > TimeSpan.FromSeconds(15))
                    {
                        this.NotifyWork();
                        break;
                    }
                }
            }
        }

        private void ScanBlock(ICoreStorage coreStorage, IChainState chainState, ChainedHeader scanBlock, bool forward, CancellationToken cancelToken = default(CancellationToken))
        {
            var replayTxes = BlockReplayer.ReplayBlock(coreStorage, chainState, scanBlock.Hash, forward, cancelToken);

            foreach (var loadedTx in replayTxes.LinkToQueue(cancelToken).GetConsumingEnumerable())
            {
                var tx = loadedTx.Transaction;
                var txIndex = loadedTx.TxIndex;

                if (!loadedTx.IsCoinbase)
                {
                    for (var inputIndex = 0; inputIndex < tx.Inputs.Length; inputIndex++)
                    {
                        var input = tx.Inputs[inputIndex];
                        var prevOutput = loadedTx.GetInputPrevTxOutput(inputIndex);
                        var prevOutputScriptHash = new UInt256(SHA256Static.ComputeHash(prevOutput.ScriptPublicKey.ToArray()));

                        var chainPosition = ChainPosition.Fake();
                        var entryType = forward ? EnumWalletEntryType.Spend : EnumWalletEntryType.UnSpend;

                        ScanForEntry(chainPosition, entryType, prevOutput, prevOutputScriptHash);
                    }
                }

                for (var outputIndex = 0; outputIndex < tx.Outputs.Length; outputIndex++)
                {
                    var output = tx.Outputs[outputIndex];
                    var outputScriptHash = new UInt256(SHA256Static.ComputeHash(output.ScriptPublicKey.ToArray()));

                    var chainPosition = ChainPosition.Fake();
                    var entryType =
                        loadedTx.IsCoinbase ?
                            (forward ? EnumWalletEntryType.Mine : EnumWalletEntryType.UnMine)
                            : (forward ? EnumWalletEntryType.Receive : EnumWalletEntryType.UnReceieve);

                    ScanForEntry(chainPosition, entryType, output, outputScriptHash);
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
                    logger.Debug("{0,-10}   {1,20:#,##0.000_000_00} BTC, Entries: {2:#,##0}".Format2(walletEntryType.ToString() + ":", txOutput.Value / (decimal)(100.MILLION()), this.entries.Count));

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
            updatedTracker.MarkStale();
            this.NotifyWork();
        }
    }
}
