# BitSharp

BitSharp intends to be a fully validating Bitcoin node written in C#. This project is currently being prototyped and should be considered alpha software.

Please refer to the [BitSharp wiki](https://github.com/pmlyon/BitSharp/wiki) for all information.

## License

BitSharp is free and unencumbered software released into the public domain.

See [LICENSE](https://github.com/pmlyon/BitSharp/blob/master/LICENSE).

# Examples

Examples can be found in the BitSharp.Examples project.

## ExampleCoreDaemon

**Source**
```csharp
public void ExampleDaemon()
{
    // create example core daemon
    BlockProvider embeddedBlocks; IStorageManager storageManager;
    using (var coreDaemon = CreateExampleDaemon(out embeddedBlocks, out storageManager, maxHeight: 99))
    using (embeddedBlocks)
    using (storageManager)
    {
        // report core daemon's progress
        logger.Info(string.Format("Core daemon height: {0:N0}", coreDaemon.CurrentChain.Height));
    }
}

private CoreDaemon CreateExampleDaemon(out BlockProvider embeddedBlocks, out IStorageManager storageManager, int? maxHeight = null)
{
    // retrieve first 10,000 testnet3 blocks
    embeddedBlocks = new BlockProvider("BitSharp.Examples.Blocks.TestNet3.zip");

    // initialize in-memory storage
    storageManager = new MemoryStorageManager();

    // intialize testnet3 rules (ignore script errors, script engine is not and is not intended to be complete)
    var rules = new Testnet3Rules { IgnoreScriptErrors = true };

    // initialize & start core daemon
    var coreDaemon = new CoreDaemon(rules, storageManager) { MaxHeight = maxHeight, IsStarted = true };

    // add embedded blocks
    coreDaemon.CoreStorage.AddBlocks(embeddedBlocks.ReadBlocks());

    // wait for core daemon to finish processing any available data
    coreDaemon.WaitForUpdate();

    return coreDaemon;
}
```

**Output**
```
Core daemon height: 99
```

## ChainStateExample

**Source**
```csharp
public void ChainStateExample()
{
    // create example core daemon
    BlockProvider embeddedBlocks; IStorageManager storageManager;
    using (var coreDaemon = CreateExampleDaemon(out embeddedBlocks, out storageManager, maxHeight: 999))
    using (embeddedBlocks)
    using (storageManager)
    // retrieve an immutable snapshot of the current chainstate, validation won't be blocked by an open snapshot
    using (var chainState = coreDaemon.GetChainState())
    {
        // retrieve unspent transactions
        var unspentTxes = chainState.ReadUnspentTransactions().ToList();

        // report counts
        logger.Info(string.Format("Chain.Height:                      {0,9:N0}", chainState.Chain.Height));
        logger.Info(string.Format("ReadUnspentTransactions().Count(): {0,9:N0}", unspentTxes.Count));
        logger.Info(string.Format("UnspentTxCount:                    {0,9:N0}", chainState.UnspentTxCount));
        logger.Info(string.Format("UnspentOutputCount:                {0,9:N0}", chainState.UnspentOutputCount));
        logger.Info(string.Format("TotalTxCount:                      {0,9:N0}", chainState.TotalTxCount));
        logger.Info(string.Format("TotalInputCount:                   {0,9:N0}", chainState.TotalInputCount));
        logger.Info(string.Format("TotalOutputCount:                  {0,9:N0}", chainState.TotalOutputCount));

        // look up genesis coinbase output (will be missing)
        UnspentTx unspentTx;
        chainState.TryGetUnspentTx(embeddedBlocks.GetBlock(0).Transactions[0].Hash, out unspentTx);
        logger.Info(string.Format("Gensis coinbase UnspentTx present? {0,9}", unspentTx != null));

        // look up block 1 coinbase output
        chainState.TryGetUnspentTx(embeddedBlocks.GetBlock(1).Transactions[0].Hash, out unspentTx);
        logger.Info(string.Format("Block 1 coinbase UnspenTx present? {0,9}", unspentTx != null));
        logger.Info(string.Format("Block 1 coinbase output states:    [{0}]", string.Join(",", unspentTx.OutputStates.Select(x => x.ToString()))));

        // look up block 381 list of spent txes
        IImmutableList<UInt256> spentTxes;
        chainState.TryGetBlockSpentTxes(381, out spentTxes);
        logger.Info(string.Format("Block 381 spent txes count:        {0,9:N0}", spentTxes.Count));
    }
}
```

**Output**
```
Chain.Height:                            999 
ReadUnspentTransactions().Count():     1,154 
UnspentTxCount:                        1,154 
UnspentOutputCount:                    1,196 
TotalTxCount:                          1,940 
TotalInputCount:                       2,918 
TotalOutputCount:                      3,115 
Gensis coinbase UnspentTx present?     False 
Block 1 coinbase UnspenTx present?      True 
Block 1 coinbase output states:    [Unspent] 
Block 381 spent txes count:               21
```