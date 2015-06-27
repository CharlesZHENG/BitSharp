# BitSharp

BitSharp intends to be a fully validating Bitcoin node written in C#. This project is currently being prototyped and should be considered alpha software.

Please refer to the [BitSharp wiki](https://github.com/pmlyon/BitSharp/wiki) for all information.

## License

BitSharp is free and unencumbered software released into the public domain.

See [LICENSE](https://github.com/pmlyon/BitSharp/blob/master/LICENSE).

# Examples

Examples can be found in the BitSharp.Examples project.

## ExampleCoreDaemon

### Source
```csharp
public void ExampleDaemon()
{
    // create example core daemon
    BlockProvider embeddedBlocks; IStorageManager storageManager;
    using (var coreDaemon = CreateExampleDaemon(out embeddedBlocks, out storageManager))
    using (embeddedBlocks)
    using (storageManager)
    {
        // report core daemon's progress
        Console.WriteLine(string.Format("Core daemon height: {0:N0}", coreDaemon.CurrentChain.Height));
    }
}

private CoreDaemon CreateExampleDaemon(out BlockProvider embeddedBlocks, out IStorageManager storageManager)
{
    // retrieve first 100 mainnet blocks
    embeddedBlocks = new BlockProvider("BitSharp.Examples.Blocks.Mainnet.zip");

    // initialize in-memory storage
    storageManager = new MemoryStorageManager();

    // initialize & start core daemon, with mainnet rules
    var coreDaemon = new CoreDaemon(new MainnetRules(), storageManager) { IsStarted = true };

    // add embedded blocks
    coreDaemon.CoreStorage.AddBlocks(embeddedBlocks.ReadBlocks());

    // wait for core daemon to finish processing any available data
    coreDaemon.WaitForUpdate();

    return coreDaemon;
}
```

### Output
```
Core daemon height: 99
```