using BitSharp.Common;
using BitSharp.Core.Domain;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.ComponentModel;
using System.Linq;
using System.Threading;

namespace BitSharp.Core
{
    public class BlockchainWalker
    {
        public BlockchainPath GetBlockchainPath(ChainedHeader fromBlock, ChainedHeader toBlock, Func<UInt256, ChainedHeader> getChainedHeader, CancellationToken? cancelToken = null)
        {
            var rewindBlocks = ImmutableList.CreateBuilder<ChainedHeader>();
            var advanceBlocks = ImmutableList.CreateBuilder<ChainedHeader>();

            foreach (var pathElement in DiscoverPath(fromBlock, toBlock, getChainedHeader, cancelToken))
            {
                switch (pathElement.Chain)
                {
                    case PathChain.From:
                        rewindBlocks.Add(pathElement.ChainedHeader);
                        break;

                    case PathChain.To:
                        advanceBlocks.Add(pathElement.ChainedHeader);
                        break;

                    case PathChain.LastCommon:
                        advanceBlocks.Reverse();
                        return new BlockchainPath(fromBlock, toBlock, pathElement.ChainedHeader, rewindBlocks.ToImmutable(), advanceBlocks.ToImmutable());

                    default:
                        throw new InvalidEnumArgumentException();
                }
            }

            throw new InvalidOperationException();
        }

        public ChainedHeader FindLastCommonBlock(ChainedHeader fromBlock, ChainedHeader toBlock, Func<UInt256, ChainedHeader> getChainedHeader, CancellationToken? cancelToken = null)
        {
            return DiscoverPath(fromBlock, toBlock, getChainedHeader, cancelToken)
                .Last().ChainedHeader;
        }

        private IEnumerable<PathElement> DiscoverPath(ChainedHeader fromBlock, ChainedHeader toBlock, Func<UInt256, ChainedHeader> getChainedHeader, CancellationToken? cancelToken)
        {
            // find height difference between chains
            var heightDelta = toBlock.Height - fromBlock.Height;

            var currentFromBlock = fromBlock;
            var currentToBlock = toBlock;

            while (currentFromBlock.Hash != currentToBlock.Hash)
            {
                // cooperative loop
                cancelToken.GetValueOrDefault(CancellationToken.None).ThrowIfCancellationRequested();

                // from chain is longer, rewind it
                if (currentFromBlock.Height > currentToBlock.Height)
                {
                    // if no further rollback is possible, chain mismatch
                    if (currentFromBlock.Height == 0)
                        throw new InvalidOperationException();

                    yield return new PathElement(PathChain.From, currentFromBlock);
                    currentFromBlock = getChainedHeader(currentFromBlock.PreviousBlockHash);
                }
                // to chain is longer, rewind it
                else if (currentToBlock.Height > currentFromBlock.Height)
                {
                    // if no further rollback is possible, chain mismatch
                    if (currentToBlock.Height == 0)
                        throw new InvalidOperationException();

                    yield return new PathElement(PathChain.To, currentToBlock);
                    currentToBlock = getChainedHeader(currentToBlock.PreviousBlockHash);
                }
                // chains are same height, rewind both
                else
                {
                    // if no further rollback is possible, chain mismatch
                    if (currentFromBlock.Height == 0 || currentToBlock.Height == 0)
                        throw new InvalidOperationException();

                    yield return new PathElement(PathChain.From, currentFromBlock);
                    yield return new PathElement(PathChain.To, currentToBlock);

                    currentFromBlock = getChainedHeader(currentFromBlock.PreviousBlockHash);
                    currentToBlock = getChainedHeader(currentToBlock.PreviousBlockHash);
                }
            }

            // return last common block
            yield return new PathElement(PathChain.LastCommon, currentFromBlock);
        }

        private class PathElement
        {

            public PathElement(PathChain chain, ChainedHeader chainedHeader)
            {
                Chain = chain;
                ChainedHeader = chainedHeader;
            }

            public PathChain Chain { get; }

            public ChainedHeader ChainedHeader { get; }
        }

        private enum PathChain
        {
            From,
            To,
            LastCommon
        }
    }
}
