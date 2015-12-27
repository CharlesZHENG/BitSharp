using BitSharp.Common;
using BitSharp.Core.Domain;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Numerics;

namespace BitSharp.Core.Builders
{
    /// <summary>
    /// This class represents a buildable chain of headers from height 0. Headers are indexed by height and by hash.
    /// </summary>
    public class ChainBuilder
    {
        // chained headers by height
        private readonly ImmutableList<ChainedHeader>.Builder blocks;

        // chained headers by hash
        private readonly ImmutableDictionary<UInt256, ChainedHeader>.Builder blocksByHash;

        /// <summary>
        /// Create an empty builder.
        /// </summary>
        public ChainBuilder()
        {
            this.blocks = ImmutableList.CreateBuilder<ChainedHeader>();
            this.blocksByHash = ImmutableDictionary.CreateBuilder<UInt256, ChainedHeader>();
        }

        /// <summary>
        /// Create a new builder from parentChain. Changes to the builder have no effect on parentChain.
        /// </summary>
        /// <param name="parentChain">The parent chain to copy.</param>
        public ChainBuilder(Chain parentChain)
        {
            this.blocks = parentChain.Blocks.ToBuilder();
            this.blocksByHash = parentChain.BlocksByHash.ToBuilder();
        }

        /// <summary>
        /// Create a new builder from an enumeration of chained headers.
        /// </summary>
        /// <param name="chainedHeaders">The chained headers, starting from height 0.</param>
        /// <exception cref="InvalidOperationException">Thrown if the headers do not form a chain from height 0.</exception>
        public ChainBuilder(IEnumerable<ChainedHeader> chainedHeaders)
        {
            this.blocks = ImmutableList.CreateBuilder<ChainedHeader>();
            this.blocksByHash = ImmutableDictionary.CreateBuilder<UInt256, ChainedHeader>();

            // add all headers to the builder
            foreach (var chainedHeader in chainedHeaders)
                this.AddBlock(chainedHeader);
        }

        /// <summary>
        /// The chain's genesis header.
        /// </summary>
        public ChainedHeader GenesisBlock => this.blocks.FirstOrDefault();

        /// <summary>
        /// The last header in the chain.
        /// </summary>
        public ChainedHeader LastBlock => this.blocks.LastOrDefault();

        /// <summary>
        /// The height of the chain. This will be one less than the count of headers.
        /// </summary>
        public int Height => this.blocks.Count() - 1;

        /// <summary>
        /// The total amount of work done on this chain.
        /// </summary>
        public BigInteger TotalWork => this.LastBlock?.TotalWork ?? 0;

        /// <summary>
        /// The list of headers in the chain, starting from height 0.
        /// </summary>
        public ImmutableList<ChainedHeader> Blocks => this.blocks.ToImmutable();

        /// <summary>
        /// The dictionary of headers in the chain, indexed by hash.
        /// </summary>
        public ImmutableDictionary<UInt256, ChainedHeader> BlocksByHash => this.blocksByHash.ToImmutable();

        /// <summary>
        /// Add a chained header to the end of this builder.
        /// </summary>
        /// <param name="chainedHeader">The chained header to add.</param>
        /// <exception cref="InvalidOperationException">Thrown if the chained header does not chain off of the last block in the chain, based on height and previous block hash.</exception>
        public ChainBuilder AddBlock(ChainedHeader chainedHeader)
        {
            var lastBlock = this.LastBlock;
            var expectedHash = lastBlock?.Hash ?? chainedHeader.PreviousBlockHash;
            var expectedHeight = (lastBlock?.Height ?? -1) + 1;

            if (chainedHeader.PreviousBlockHash != expectedHash
                || chainedHeader.Height != expectedHeight)
                throw new InvalidOperationException();

            this.blocks.Add(chainedHeader);
            this.blocksByHash.Add(chainedHeader.Hash, chainedHeader);

            return this;
        }

        /// <summary>
        /// Remove a chained header to the end of this builder.
        /// </summary>
        /// <param name="chainedHeader">The chained header to remove.</param>
        /// <exception cref="InvalidOperationException">Thrown if the chained header is not the last block in the chain.</exception>
        public ChainBuilder RemoveBlock(ChainedHeader chainedHeader)
        {
            var lastBlock = this.LastBlock;
            if (lastBlock == null
                || chainedHeader.Hash != lastBlock.Hash)
                throw new InvalidOperationException();

            this.blocks.RemoveAt(this.blocks.Count - 1);
            this.blocksByHash.Remove(chainedHeader.Hash);

            return this;
        }

        public IEnumerable<Tuple<int, ChainedHeader>> NavigateTowards(Chain chain)
        {
            return NavigateTowards(() => chain);
        }

        public IEnumerable<Tuple<int, ChainedHeader>> NavigateTowards(Func<Chain> chain)
        {
            Tuple<int, ChainedHeader> singleStep;
            do
            {
                singleStep = this.ToImmutable().NavigateTowards(chain()).FirstOrDefault();
                if (singleStep != null)
                    yield return singleStep;
            }
            while (singleStep != null);
        }

        /// <summary>
        /// Create a new Chain instance from this builder. Changes to the builder have no effect on the new chain.
        /// </summary>
        /// <returns>The chain instance.</returns>
        public Chain ToImmutable()
        {
            return new Chain(this.blocks.ToImmutable(), this.blocksByHash.ToImmutable());
        }
    }
}
