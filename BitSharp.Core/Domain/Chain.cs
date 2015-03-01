using BitSharp.Common;
using BitSharp.Core.Builders;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Numerics;

namespace BitSharp.Core.Domain
{
    /// <summary>
    /// This class represents an immutable and valid, contiguous chain of headers from height 0. Headers are indexed by height and by hash.
    /// </summary>
    public class Chain
    {
        // chained headers by height
        private readonly ImmutableList<ChainedHeader> blocks;

        // chained headers by hash
        private readonly ImmutableDictionary<UInt256, ChainedHeader> blocksByHash;

        // constructor, both headers counts must match
        internal Chain(ImmutableList<ChainedHeader> blocks, ImmutableDictionary<UInt256, ChainedHeader> blocksByHash)
        {
            if (blocks == null)
                throw new ArgumentNullException("blocks");
            if (blocksByHash == null)
                throw new ArgumentNullException("blocksByHash");
            if (blocks.Count != blocksByHash.Count)
                throw new ArgumentException();

            this.blocks = blocks;
            this.blocksByHash = blocksByHash;
        }

        /// <summary>
        /// The chain's genesis header.
        /// </summary>
        public ChainedHeader GenesisBlock { get { return this.blocks.FirstOrDefault(); } }

        /// <summary>
        /// The last header in the chain.
        /// </summary>
        public ChainedHeader LastBlock { get { return this.blocks.LastOrDefault(); } }

        /// <summary>
        /// The height of the chain. This will be one less than the count of headers.
        /// </summary>
        public int Height { get { return this.blocks.Count() - 1; } }

        /// <summary>
        /// The total amount of work done on this chain.
        /// </summary>
        public BigInteger TotalWork { get { return this.LastBlock != null ? this.LastBlock.TotalWork : 0; } }

        /// <summary>
        /// The list of headers in the chain, starting from height 0.
        /// </summary>
        public ImmutableList<ChainedHeader> Blocks { get { return this.blocks; } }

        /// <summary>
        /// The dictionary of headers in the chain, indexed by hash.
        /// </summary>
        public ImmutableDictionary<UInt256, ChainedHeader> BlocksByHash { get { return this.blocksByHash; } }

        /// <summary>
        /// Enumerate the path of headers to get from this chain to the target chain.
        /// </summary>
        /// <param name="targetChain">The chain to navigate towards.</param>
        /// <returns>An enumeration of the path's headers.</returns>
        /// <exception cref="InvalidOperationException">Thrown if there is no path to the target chain.</exception>
        public IEnumerable<Tuple<int, ChainedHeader>> NavigateTowards(Chain targetChain)
        {
            return this.NavigateTowards(() => targetChain);
        }

        /// <summary>
        /// Enumerate the path of headers to get from this chain to a target chain, with the target chain updating after each yield.
        /// </summary>
        /// <param name="targetChain">The function to return the chain to navigate towards.</param>
        /// <returns>An enumeration of the path's headers.</returns>
        /// <exception cref="InvalidOperationException">Thrown if there is no path to the target chain.</exception>
        /// <remarks>
        /// <para>The path from block 2 to block 4 would consist of [+1,block3], [+1,block4].</para>
        /// <para>The path from block 4a to block 3b, with a last common ancestor of block 1, would
        /// consist of: [-1,block4a],[-1,block3a], [-1,block2a], [+1,block2b], [+1,block3b]. Note that the last
        /// common ancestor is not listed.</para>
        /// </remarks>
        public IEnumerable<Tuple<int, ChainedHeader>> NavigateTowards(Func<Chain> getTargetChain)
        {
            var currentBlock = this.LastBlock;
            var genesisBlock = currentBlock != null ? this.GenesisBlock : null;

            while (true)
            {
                // acquire the target chain
                var targetChain = getTargetChain();

                // no target chain, stop navigating
                if (targetChain == null)
                    yield break;
                // if empty target chain, stop navigating
                else if (targetChain.Height == -1)
                    yield break;
                // verify the genesis block of the target chain matches current chain
                else if (genesisBlock != null && genesisBlock != targetChain.GenesisBlock)
                    throw new InvalidOperationException();

                // if no current block, add genesis
                if (currentBlock == null)
                {
                    genesisBlock = targetChain.GenesisBlock;
                    currentBlock = genesisBlock;
                    yield return Tuple.Create(+1, currentBlock);
                }
                // if currently ahead of target chain, must rewind
                else if (currentBlock.Height > targetChain.Height)
                {
                    if (currentBlock.Height == 0)
                        throw new InvalidOperationException();

                    yield return Tuple.Create(-1, currentBlock);
                    currentBlock = this.blocks[currentBlock.Height - 1];
                }
                // currently behind target chain
                else
                {
                    // on same chain, can advance
                    if (targetChain.Blocks[currentBlock.Height] == currentBlock)
                    {
                        // another block is available
                        if (targetChain.Height >= currentBlock.Height + 1)
                        {
                            currentBlock = targetChain.Blocks[currentBlock.Height + 1];
                            yield return Tuple.Create(+1, currentBlock);
                        }
                        // no further blocks are available
                        else
                        {
                            yield break;
                        }
                    }
                    // on different chains, must rewind
                    else
                    {
                        if (currentBlock.Height == 0)
                            throw new InvalidOperationException();

                        yield return Tuple.Create(-1, currentBlock);
                        currentBlock = this.blocks[currentBlock.Height - 1];
                    }
                }
            }
        }

        /// <summary>
        /// Create a new ChainBuilder instance from this chain. Changes to the builder have no effect on this chain.
        /// </summary>
        /// <returns>The builder instance.</returns>
        public ChainBuilder ToBuilder()
        {
            return new ChainBuilder(this);
        }

        /// <summary>
        /// Create a new Chain instance consisting of a single genesis header.
        /// </summary>
        /// <param name="genesisBlock">The genesis header.</param>
        /// <returns>The Chain instance.</returns>
        public static Chain CreateForGenesisBlock(ChainedHeader genesisBlock)
        {
            var chainBuilder = new ChainBuilder();
            chainBuilder.AddBlock(genesisBlock);
            return chainBuilder.ToImmutable();
        }
    }
}
