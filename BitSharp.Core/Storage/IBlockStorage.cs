using BitSharp.Common;
using BitSharp.Core.Domain;
using System;
using System.Collections.Generic;

namespace BitSharp.Core.Storage
{
    public interface IBlockStorage : IDisposable
    {
        /// <summary>
        /// Determines whether a chained header exists in storage.
        /// </summary>
        /// <param name="blockHash">The header's hash.</param>
        /// <returns>true if the header is present; otherwise, false</returns>
        bool ContainsChainedHeader(UInt256 blockHash);

        /// <summary>
        /// Add a chained header to storage.
        /// </summary>
        /// <param name="chainedHeader">The header to add.</param>
        /// <returns>true if the header was added; otherwise, false</returns>
        bool TryAddChainedHeader(ChainedHeader chainedHeader);

        /// <summary>
        /// Retrieve a chained header from storage.
        /// </summary>
        /// <param name="blockHash">The header's hash.</param>
        /// <param name="chainedHeader">Contains the retrieved header when successful; otherwise, null.</param>
        /// <returns>true if the header was retrieved; otherwise, false</returns>
        bool TryGetChainedHeader(UInt256 blockHash, out ChainedHeader chainedHeader);

        /// <summary>
        /// Remove a chained header to storage.
        /// </summary>
        /// <param name="blockHash">The header's hash.</param>
        /// <returns>true if the header was removed; otherwise, false</returns>
        bool TryRemoveChainedHeader(UInt256 blockHash);
        
        /// <summary>
        /// Find the chained header with the highest total work value for its chain. Only valid blocks will be returned.
        /// </summary>
        /// <returns>The header with the most work.</returns>
        ChainedHeader FindMaxTotalWork();

        /// <summary>
        /// Enumerate all chained headers in storage.
        /// </summary>
        /// <returns>An enumerable of all headers.</returns>
        IEnumerable<ChainedHeader> ReadChainedHeaders();

        /// <summary>
        /// Query if a block has been marked invalid, either due to an invalid header or block contents.
        /// </summary>
        /// <param name="blockHash">The block's hash.</param>
        /// <returns>true if the block is invalid; otherwise, false</returns>
        bool IsBlockInvalid(UInt256 blockHash);

        /// <summary>
        /// Mark a block as being invalid, either due to an invalid header or block contents.
        /// </summary>
        /// <param name="blockHash">The block's hash.</param>
        void MarkBlockInvalid(UInt256 blockHash);

        /// <summary>
        /// Fully flush storage.
        /// </summary>
        void Flush();

        //TODO keep this here? IStorageManager?
        void Defragment();
    }
}
