using BitSharp.Core.Builders;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace BitSharp.Core.Domain
{
    /// <summary>
    /// Represents the list of transaction spent within a block, grouped by the originating block of each transaction.
    /// </summary>
    public class BlockSpentTxes : IReadOnlyList<SpentTx>
    {
        private readonly IImmutableList<SpentTx> spentTxesByBlock;

        internal BlockSpentTxes(IImmutableList<SpentTx> spentTxesByBlock)
        {
            this.spentTxesByBlock = spentTxesByBlock;
        }

        public int Count
        {
            get { return spentTxesByBlock.Count; }
        }

        public SpentTx this[int index]
        {
            get { return spentTxesByBlock[index]; }
        }

        public IEnumerator<SpentTx> GetEnumerator()
        {
            return spentTxesByBlock.GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return spentTxesByBlock.GetEnumerator();
        }

        public IEnumerable<Tuple<int, IImmutableList<SpentTx>>> ReadByBlock()
        {
            var currentBlockIndex = -1;
            ImmutableList<SpentTx>.Builder currentSpentTxes = null;

            foreach (var spentTx in spentTxesByBlock)
            {
                var blockIndex = spentTx.ConfirmedBlockIndex;

                // detect change in current block index, complete the current spent tx list and start a new one
                if (currentBlockIndex != blockIndex)
                {
                    if (currentSpentTxes != null)
                        yield return Tuple.Create(currentBlockIndex, (IImmutableList<SpentTx>)currentSpentTxes.ToImmutable());

                    currentBlockIndex = blockIndex;
                    currentSpentTxes = ImmutableList.CreateBuilder<SpentTx>();
                }

                currentSpentTxes.Add(spentTx);
            }

            // complete the current spent tx list before exiting
            if (currentSpentTxes != null)
                yield return Tuple.Create(currentBlockIndex, (IImmutableList<SpentTx>)currentSpentTxes.ToImmutable());
        }

        public static BlockSpentTxes CreateRange(IEnumerable<SpentTx> spentTxes)
        {
            var builder = new BlockSpentTxesBuilder();
            foreach (var spentTx in spentTxes)
                builder.AddSpentTx(spentTx);

            return builder.ToImmutable();
        }
    }
}
