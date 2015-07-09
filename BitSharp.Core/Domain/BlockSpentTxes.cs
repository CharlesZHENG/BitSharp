using BitSharp.Core.Builders;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

        public static BlockSpentTxes CreateRange(IEnumerable<SpentTx> spentTxes)
        {
            var builder = new BlockSpentTxesBuilder();
            foreach (var spentTx in spentTxes)
                builder.AddSpentTx(spentTx);

            return builder.ToImmutable();
        }
    }
}
